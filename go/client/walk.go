package client

import (
	"io/fs"
	"path"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

// walkPage is the entry count asked for in each Ls request of a walk.
const walkPage = 1024

// ReadDirAll lists every entry of path with attributes, in server order.
func (cl *Client) ReadDirAll(path string) ([]*DirEntryWithInfo, error) {
	var all []*DirEntryWithInfo
	cursor := ""
	for {
		entries, next, err := cl.Ls(path, walkPage, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, entries...)
		if next == "" {
			return all, nil
		}
		cursor = next
	}
}

// WalkDir calls fn for every entry below root, a directory before its
// contents, in server order. root itself is not reported. p is the entry path
// and e carries its attributes from Ls, so fn needs no Stat. Symlinks are
// reported and not followed. Listings use paged Ls requests.
//
// When fn returns fs.SkipDir for a directory, WalkDir does not enter it. Any
// other error from fn stops the walk and WalkDir returns it. A directory that
// cannot be listed also stops the walk, with that error. No entry is skipped
// silently.
func (cl *Client) WalkDir(root string, fn func(p string, e *DirEntryWithInfo) error) error {
	return cl.walk(root, fn)
}

func (cl *Client) walk(dir string, fn func(string, *DirEntryWithInfo) error) error {
	cursor := ""
	for {
		entries, next, err := cl.Ls(dir, walkPage, cursor)
		if err != nil {
			return err
		}
		for _, e := range entries {
			p := path.Join(dir, e.GetName())
			isDir := e.GetType() == wire.FileType_FILE_TYPE_DIRECTORY
			switch err := fn(p, e); {
			case err == fs.SkipDir:
			case err != nil:
				return err
			case isDir:
				if err := cl.walk(p, fn); err != nil {
					return err
				}
			}
		}
		if next == "" {
			return nil
		}
		cursor = next
	}
}
