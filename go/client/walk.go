package client

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

// walkPage is the entry count asked for in each Ls request of a walk.
const walkPage = 1024

// Bounds for the directory helpers. A per-call option overrides each one.
const (
	// DefaultMaxDirEntries is the most entries ReadDirAll holds.
	DefaultMaxDirEntries = 1_000_000
	// DefaultMaxWalkDepth is the most directory levels WalkDir descends below its root.
	DefaultMaxWalkDepth = 1024
)

// maxEmptyPages is the most consecutive empty pages a listing may return before the helpers stop.
const maxEmptyPages = 16

// ErrTooManyEntries matches the error ReadDirAll returns when a directory has more entries than its limit.
var ErrTooManyEntries = errors.New("mountos: too many directory entries")

// ErrWalkTooDeep matches the error WalkDir returns when the tree is deeper than its limit.
var ErrWalkTooDeep = errors.New("mountos: directory tree too deep")

// TooManyEntriesError reports a directory with more than Limit entries.
type TooManyEntriesError struct {
	Path  string
	Limit int
}

func (e *TooManyEntriesError) Error() string {
	return fmt.Sprintf("mountos: %s has more than %d entries", e.Path, e.Limit)
}

// Is matches ErrTooManyEntries.
func (e *TooManyEntriesError) Is(target error) bool { return target == ErrTooManyEntries }

// WalkTooDeepError reports a tree deeper than Limit levels below the walk root.
type WalkTooDeepError struct {
	Path  string
	Limit int
}

func (e *WalkTooDeepError) Error() string {
	return fmt.Sprintf("mountos: %s is deeper than %d levels", e.Path, e.Limit)
}

// Is matches ErrWalkTooDeep.
func (e *WalkTooDeepError) Is(target error) bool { return target == ErrWalkTooDeep }

type dirOpts struct {
	ctx        context.Context //nolint:containedctx // Per-call option, not stored beyond the call.
	maxEntries int
	maxDepth   int
}

// DirOption changes a limit of ReadDirAll or WalkDir for one call.
type DirOption func(*dirOpts)

// WithMaxEntries sets the most entries ReadDirAll holds. A value of zero or less keeps the default.
func WithMaxEntries(n int) DirOption { return func(o *dirOpts) { o.maxEntries = n } }

// WithMaxDepth sets the most directory levels WalkDir descends below its root. A value of zero or
// less keeps the default.
func WithMaxDepth(n int) DirOption { return func(o *dirOpts) { o.maxDepth = n } }

// WithContext stops ReadDirAll or WalkDir with ctx.Err() once ctx is done. The check runs before each
// Ls request and each entry.
func WithContext(ctx context.Context) DirOption { return func(o *dirOpts) { o.ctx = ctx } }

func newDirOpts(opts []DirOption) dirOpts {
	o := dirOpts{ctx: context.Background(), maxEntries: DefaultMaxDirEntries, maxDepth: DefaultMaxWalkDepth}
	var set dirOpts
	for _, f := range opts {
		f(&set)
	}
	if set.maxEntries > 0 {
		o.maxEntries = set.maxEntries
	}
	if set.maxDepth > 0 {
		o.maxDepth = set.maxDepth
	}
	if set.ctx != nil {
		o.ctx = set.ctx
	}
	return o
}

type lsFunc func(path string, limit uint32, cursor string) ([]*DirEntryWithInfo, string, error)

// pager follows the cursor chain of one directory listing.
type pager struct {
	ctx    context.Context //nolint:containedctx // Lives for one listing.
	dir    string
	cursor string
	empty  int // consecutive empty pages
}

// page fetches the next page. It returns the entries and whether more pages follow. A cursor that
// repeats, or too many empty pages in a row, ends the listing with a *ProtocolError.
func (g *pager) page(ls lsFunc) ([]*DirEntryWithInfo, bool, error) {
	if err := g.ctx.Err(); err != nil {
		return nil, false, err
	}
	entries, next, err := ls(g.dir, walkPage, g.cursor)
	if err != nil {
		return nil, false, err
	}
	if next == "" {
		return entries, false, nil
	}
	if next == g.cursor {
		return nil, false, &ProtocolError{Op: "ls", Err: fmt.Errorf("cursor %q of %s did not advance", next, g.dir)}
	}
	if len(entries) > 0 {
		g.empty = 0
	} else if g.empty++; g.empty > maxEmptyPages {
		return nil, false, &ProtocolError{Op: "ls", Err: fmt.Errorf("%d empty pages in a row in %s", g.empty, g.dir)}
	}
	g.cursor = next
	return entries, true, nil
}

// ReadDirAll lists every entry of path with attributes, in server order. It
// returns a *TooManyEntriesError (matching ErrTooManyEntries) when the
// directory has more entries than the limit, before it holds them.
func (cl *Client) ReadDirAll(path string, opts ...DirOption) ([]*DirEntryWithInfo, error) {
	o := newDirOpts(opts)
	return readDirAll(o.ctx, cl.Ls, path, o.maxEntries)
}

func readDirAll(ctx context.Context, ls lsFunc, path string, limit int) ([]*DirEntryWithInfo, error) {
	var all []*DirEntryWithInfo
	g := &pager{ctx: ctx, dir: path}
	for {
		entries, more, err := g.page(ls)
		if err != nil {
			return nil, err
		}
		if len(all)+len(entries) > limit {
			return nil, &TooManyEntriesError{Path: path, Limit: limit}
		}
		all = append(all, entries...)
		if !more {
			return all, nil
		}
	}
}

// WalkDir calls fn for every entry below root, a directory before its
// contents, in server order. root itself is not reported. p is the entry path
// and e carries its attributes from Ls, so fn needs no Stat. Symlinks are
// reported and not followed. Listings use paged Ls requests, so the walk holds
// one page per directory level on the path to the current entry.
//
// When fn returns fs.SkipDir for a directory, WalkDir does not enter it. Any
// other error from fn stops the walk and WalkDir returns it. A directory that
// cannot be listed also stops the walk, with that error. No entry is skipped
// silently. A tree deeper than the limit stops the walk with a
// *WalkTooDeepError (matching ErrWalkTooDeep).
func (cl *Client) WalkDir(root string, fn func(p string, e *DirEntryWithInfo) error, opts ...DirOption) error {
	o := newDirOpts(opts)
	return walkDir(o.ctx, cl.Ls, root, 0, o.maxDepth, fn)
}

func walkDir(ctx context.Context, ls lsFunc, dir string, depth, maxDepth int, fn func(string, *DirEntryWithInfo) error) error {
	g := &pager{ctx: ctx, dir: dir}
	for {
		entries, more, err := g.page(ls)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			p := path.Join(dir, e.GetName())
			isDir := e.GetType() == wire.FileType_FILE_TYPE_DIRECTORY
			switch err := fn(p, e); {
			case errors.Is(err, fs.SkipDir):
			case err != nil:
				return err
			case isDir:
				if depth+1 > maxDepth {
					return &WalkTooDeepError{Path: p, Limit: maxDepth}
				}
				if err := walkDir(ctx, ls, p, depth+1, maxDepth, fn); err != nil {
					return err
				}
			}
		}
		if !more {
			return nil
		}
	}
}
