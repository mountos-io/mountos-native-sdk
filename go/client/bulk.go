package client

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

// maxBatchItems is the most paths or ranges the server accepts in one batch request.
const maxBatchItems = 4096

// StatResult is the outcome for one path of StatBatch. Info is nil when Err is set.
type StatResult struct {
	_    struct{} // Callers must name fields. New fields can be added.
	Info *wire.FileInfo
	Err  error
}

// Range is a byte range of a file for ReadBatch.
type Range struct {
	_      struct{} // Callers must name fields. New fields can be added.
	Offset int64
	Length uint32
}

// ReadResult is the outcome for one Range. Data is shorter than the range
// length when the range reaches past end of file.
type ReadResult struct {
	_    struct{} // Callers must name fields. New fields can be added.
	Err  error
	Data []byte
}

// WriteItem is data to write at Offset for WriteBatch.
type WriteItem struct {
	_      struct{} // Callers must name fields. New fields can be added.
	Data   []byte
	Offset int64
}

// WriteResult is the outcome for one WriteItem. N is the number of bytes written.
type WriteResult struct {
	_   struct{} // Callers must name fields. New fields can be added.
	Err error
	N   int
}

// itemErrors maps the batch error list to errors by position. Every index must
// be below n.
func itemErrors(list []*wire.BatchItemError, n int, op string) (map[int]error, error) {
	if len(list) == 0 {
		return nil, nil //nolint:nilnil // A nil map means no item failed.
	}
	m := make(map[int]error, len(list))
	for _, e := range list {
		if int(e.GetIndex()) >= n {
			return nil, &ProtocolError{Op: op, Err: fmt.Errorf("error index %d out of %d items", e.GetIndex(), n)}
		}
		m[int(e.GetIndex())] = &WireError{Code: e.GetCode(), Message: e.GetMessage()}
	}
	return m, nil
}

// statChunks splits paths into requests of at most maxBatchItems paths whose
// encoded size stays well under the metadata limit.
func statChunks(paths []string) [][]string {
	var out [][]string
	for len(paths) > 0 {
		n, bytes := 0, 0
		for n < len(paths) && n < maxBatchItems && (n == 0 || bytes+len(paths[n])+pathOverhead <= maxMetaBytes/2) {
			bytes += len(paths[n]) + pathOverhead
			n++
		}
		out = append(out, paths[:n])
		paths = paths[n:]
	}
	return out
}

// pathOverhead is the encoded size of a path beyond its bytes: tag and length.
const pathOverhead = 8

// StatBatch stats every path and returns one result per path, in order. A
// failed path sets Err for that entry only. Large inputs are split into
// several requests that run in parallel across the pool.
func (cl *Client) StatBatch(paths []string) ([]StatResult, error) {
	chunks := statChunks(paths)
	parts := make([][]StatResult, len(chunks))
	err := parallelDo(len(cl.conns), len(chunks), func(_, ci int) error {
		part := chunks[ci]
		var r wire.StatBatchResponse
		if err := cl.run(mStatBatch, &wire.StatBatchRequest{Paths: part}, &r); err != nil {
			return err
		}
		if len(r.GetInfos()) != len(part) {
			return &ProtocolError{Op: "stat batch", Err: fmt.Errorf("%d infos for %d paths", len(r.GetInfos()), len(part))}
		}
		errs, err := itemErrors(r.GetErrors(), len(part), "stat batch")
		if err != nil {
			return err
		}
		res := make([]StatResult, len(part))
		for i, info := range r.GetInfos() {
			if e, ok := errs[i]; ok {
				res[i] = StatResult{Err: e}
			} else {
				res[i] = StatResult{Info: info}
			}
		}
		parts[ci] = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]StatResult, 0, len(paths))
	for _, p := range parts {
		out = append(out, p...)
	}
	return out, nil
}

// parallelDo runs fn for every index below n on up to workers goroutines. fn
// gets its worker number, which is below workers, so a worker can own a
// resource. After the first error no new index starts. It returns that error.
func parallelDo(workers, n int, fn func(worker, i int) error) error {
	var next atomic.Int64
	var failed atomic.Bool
	var once sync.Once
	var first error
	var wg sync.WaitGroup
	for w := 0; w < min(workers, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !failed.Load() {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				if err := fn(w, i); err != nil {
					once.Do(func() { first = err })
					failed.Store(true)
				}
			}
		}()
	}
	wg.Wait()
	return first
}

// piece is a slice of one caller item, small enough to travel in one frame.
type piece struct {
	owner  int
	offset int64
	length int
	src    int // offset into the owner's Data (writes)
}

// pieces splits items of the given lengths so that no piece exceeds limit bytes.
// A zero-length item still yields one piece so it gets a result.
func pieces(offsets []int64, lengths []int, limit int) []piece {
	var out []piece
	for i, n := range lengths {
		if n == 0 {
			out = append(out, piece{owner: i, offset: offsets[i]})
			continue
		}
		for done := 0; done < n; {
			l := min(limit, n-done)
			out = append(out, piece{owner: i, offset: offsets[i] + int64(done), length: l, src: done})
			done += l
		}
	}
	return out
}

// groups cuts pieces into requests of at most maxBatchItems pieces and limit total bytes.
func groups(ps []piece, limit int) [][]piece {
	var out [][]piece
	for len(ps) > 0 {
		n, bytes := 0, 0
		for n < len(ps) && n < maxBatchItems && bytes+ps[n].length <= limit {
			bytes += ps[n].length
			n++
		}
		out = append(out, ps[:n])
		ps = ps[n:]
	}
	return out
}

func wireRanges(ps []piece) []*wire.Range {
	rs := make([]*wire.Range, len(ps))
	for i, p := range ps {
		rs[i] = &wire.Range{Offset: p.offset, Length: uint32(p.length)}
	}
	return rs
}

// overlaps reports whether any two non-empty ranges share a byte.
func overlaps(offsets []int64, lengths []int) bool {
	idx := make([]int, 0, len(offsets))
	for i, n := range lengths {
		if n > 0 {
			idx = append(idx, i)
		}
	}
	slices.SortFunc(idx, func(a, b int) int { return int(offsets[a] - offsets[b]) })
	var end int64
	for k, i := range idx {
		if k > 0 && offsets[i] < end {
			return true
		}
		end = max(end, offsets[i]+int64(lengths[i]))
	}
	return false
}

// fanout returns handles to the same file, so batch groups can run in
// parallel. f comes first. The server runs requests on one handle in order, so
// parallel groups need their own handles. When the connection allows several
// requests in flight, the extra handles live on f's connection. Otherwise they
// live on other pooled connections. A handle counts only when its inode equals
// f's, so a path that names another file is never used. When the server gives
// no inode, or any step fails, the batch stays on f alone. The caller holds
// f.mu and calls release on the result.
func (f *File) fanout(groupCount int) []*File {
	files := []*File{f}
	want := min(groupCount, len(f.cl.conns))
	if want < 2 || f.path == "" {
		return files
	}
	info, err := f.fstat()
	if err != nil || info.GetInode() == 0 {
		return files
	}
	extra := func(sk *sock) {
		if !sk.hold() {
			return
		}
		sf, err := f.cl.openOn(sk, f.path, f.flags&(3|wire.OpenFlag_OPEN_FLAG_APPEND), 0)
		sk.release()
		if err != nil {
			return
		}
		if si, err := sf.fstat(); err != nil || si.GetInode() != info.GetInode() {
			_ = sf.Close() // The handle is discarded; a close error changes nothing.
			return
		}
		files = append(files, sf)
	}
	if f.sk.limit > 1 {
		for len(files) < want && !f.sk.isDead() {
			n := len(files)
			extra(f.sk)
			if len(files) == n {
				break
			}
		}
		return files
	}
	for _, c := range f.cl.conns {
		if len(files) == want {
			break
		}
		if c == f.sk.c {
			continue
		}
		if sk, err := c.session(); err == nil {
			extra(sk)
		}
	}
	return files
}

// release closes the handles fanout opened and returns the first close error.
func release(files []*File) error {
	var first error
	for _, sf := range files[1:] {
		if err := sf.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// fanoutMinBytes is the smallest ReadBatch size worth the extra opens and
// stats that parallel connections cost.
var fanoutMinBytes = 256 << 10

// readGroup is the checked reply to one ReadBatch request.
type readGroup struct {
	errs   map[int]error
	counts []uint32
	data   []byte
}

// ReadBatch reads several ranges of the file in as few requests as the server
// limits allow. Requests run in parallel on several connections when the file
// can be opened again by path. Results are in range order. A range past end of
// file returns fewer bytes than its length, and bytes after a short or failed
// part of a range are dropped. The File position is not used or changed.
// Data of a range read in one piece shares the reply buffer with other ranges
// of the same request, so it stays allocated while any such Data is in use.
func (f *File) ReadBatch(ranges []Range) ([]ReadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offsets := make([]int64, len(ranges))
	lengths := make([]int, len(ranges))
	total := 0
	for i, r := range ranges {
		if r.Offset < 0 {
			return nil, fmt.Errorf("mountos: negative offset in range %d", i)
		}
		offsets[i], lengths[i] = r.Offset, int(r.Length)
		total += int(r.Length)
	}
	gs := groups(pieces(offsets, lengths, f.chunk()), f.chunk())
	files := []*File{f}
	if total >= fanoutMinBytes {
		files = f.fanout(len(gs))
	}
	outs := make([]readGroup, len(gs))
	err := parallelDo(len(files), len(gs), func(w, gi int) error {
		g := gs[gi]
		var r wire.ReadBatchResponse
		data, err := files[w].do(mReadBatch, &wire.ReadBatchRequest{Handle: files[w].handle, Ranges: wireRanges(g)}, &r, nil)
		if err != nil {
			return err
		}
		if len(r.GetCounts()) != len(g) {
			return &ProtocolError{Op: "read batch", Err: fmt.Errorf("%d counts for %d ranges", len(r.GetCounts()), len(g))}
		}
		errs, err := itemErrors(r.GetErrors(), len(g), "read batch")
		if err != nil {
			return err
		}
		pos := 0
		for i, p := range g {
			n := int(r.GetCounts()[i])
			if n > p.length || pos+n > len(data) {
				return &ProtocolError{Op: "read batch", Err: fmt.Errorf("count %d for range %d of %d bytes, payload %d", n, i, p.length, len(data))}
			}
			pos += n
		}
		if pos != len(data) {
			return &ProtocolError{Op: "read batch", Err: fmt.Errorf("payload %d bytes, counts sum %d", len(data), pos)}
		}
		outs[gi] = readGroup{counts: r.GetCounts(), data: data, errs: errs}
		return nil
	})
	_ = release(files) // The read result is final; a close error on read-only handles changes nothing.
	if err != nil {
		return nil, err
	}
	res := make([]ReadResult, len(ranges))
	short := make([]bool, len(ranges)) // a part came back short or failed, so later parts would leave a hole
	for gi, g := range gs {
		o := outs[gi]
		pos := 0
		for i, p := range g {
			n := int(o.counts[i])
			out := &res[p.owner]
			if !short[p.owner] {
				if out.Data == nil && n > 0 {
					// The first part aliases the reply buffer. The capacity cap makes a
					// later part or a caller append copy instead of writing a neighbor.
					out.Data = o.data[pos : pos+n : pos+n]
				} else {
					out.Data = append(out.Data, o.data[pos:pos+n]...)
				}
				short[p.owner] = n < p.length
			}
			pos += n
			if e, ok := o.errs[i]; ok && out.Err == nil {
				out.Err = e
				short[p.owner] = true
			}
		}
	}
	return res, nil
}

// writeGroup is the checked reply to one WriteBatch request.
type writeGroup struct {
	errs   map[int]error
	counts []uint32
}

// WriteBatch writes several items in as few requests as the server limits
// allow. Requests run in parallel on several connections when the file can be
// opened again by path and no two items overlap. A File opened for append runs
// the requests one at a time in item order, since the server ignores offsets
// there. Results are in item order. The File position is not used or changed.
func (f *File) WriteBatch(items []WriteItem) ([]WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offsets := make([]int64, len(items))
	lengths := make([]int, len(items))
	for i, it := range items {
		if it.Offset < 0 {
			return nil, fmt.Errorf("mountos: negative offset in item %d", i)
		}
		offsets[i], lengths[i] = it.Offset, len(it.Data)
	}
	gs := groups(pieces(offsets, lengths, f.chunk()), f.chunk())
	files := []*File{f}
	if len(gs) > 1 && f.flags&wire.OpenFlag_OPEN_FLAG_APPEND == 0 && !overlaps(offsets, lengths) {
		files = f.fanout(len(gs))
	}
	outs := make([]writeGroup, len(gs))
	err := parallelDo(len(files), len(gs), func(w, gi int) error {
		g := gs[gi]
		total := 0
		for _, p := range g {
			total += p.length
		}
		payload := make([]byte, 0, total)
		for _, p := range g {
			payload = append(payload, items[p.owner].Data[p.src:p.src+p.length]...)
		}
		var r wire.WriteBatchResponse
		if _, err := files[w].do(mWriteBatch, &wire.WriteBatchRequest{Handle: files[w].handle, Ranges: wireRanges(g)}, &r, payload); err != nil {
			return err
		}
		if len(r.GetCounts()) != len(g) {
			return &ProtocolError{Op: "write batch", Err: fmt.Errorf("%d counts for %d ranges", len(r.GetCounts()), len(g))}
		}
		errs, err := itemErrors(r.GetErrors(), len(g), "write batch")
		if err != nil {
			return err
		}
		for i, p := range g {
			if n := int(r.GetCounts()[i]); n > p.length {
				return &ProtocolError{Op: "write batch", Err: fmt.Errorf("count %d exceeds sent %d", n, p.length)}
			}
		}
		outs[gi] = writeGroup{counts: r.GetCounts(), errs: errs}
		return nil
	})
	cerr := release(files)
	if err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	res := make([]WriteResult, len(items))
	for gi, g := range gs {
		o := outs[gi]
		for i, p := range g {
			n := int(o.counts[i])
			out := &res[p.owner]
			out.N += n
			if e, ok := o.errs[i]; ok && out.Err == nil {
				out.Err = e
			} else if n < p.length && out.Err == nil {
				out.Err = io.ErrShortWrite
			}
		}
	}
	return res, nil
}

// ReadFile returns the whole content of path. One request returns up to the
// payload limit. For a larger file ReadFile opens it and reads the rest in
// chunks from offset zero on that one descriptor, so the result comes from a
// single inode.
func (cl *Client) ReadFile(path string) ([]byte, error) {
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	sk, err := cl.acquire()
	if err != nil {
		return nil, err
	}
	limit := sk.maxData
	var r wire.ReadFileResponse
	data, err := sk.call(mReadFile, &wire.ReadFileRequest{Path: path, MaxBytes: uint64(limit)}, &r, nil)
	sk.release()
	if err != nil {
		return nil, err
	}
	if int(r.GetCount()) != len(data) || len(data) > limit {
		return nil, &ProtocolError{Op: "read file", Err: fmt.Errorf("count %d, payload %d, limit %d", r.GetCount(), len(data), limit)}
	}
	if len(data) < limit {
		return data, nil
	}
	f, err := cl.Open(path, wire.OpenFlag_OPEN_FLAG_READ_ONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // Read-only handle; a close error changes nothing.
	buf := make([]byte, limit)
	var all []byte
	for {
		n, err := f.ReadAt(buf, int64(len(all)))
		all = append(all, buf[:n]...)
		if errors.Is(err, io.EOF) {
			return all, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// WriteFile writes data to path and creates the file when missing. With
// truncate the file holds exactly data afterward. Without it, bytes beyond
// len(data) stay. Data over the payload limit goes through Open and WriteAt.
func (cl *Client) WriteFile(path string, data []byte, truncate bool) error {
	if cl.isClosed() {
		return ErrClientClosed
	}
	sk, err := cl.acquire()
	if err != nil {
		return err
	}
	if len(data) <= sk.maxData {
		var r wire.WriteFileResponse
		_, err = sk.call(mWriteFile, &wire.WriteFileRequest{Path: path, Truncate: truncate}, &r, data)
		sk.release()
		if err != nil {
			return err
		}
		if int(r.GetCount()) != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	sk.release()
	flags := wire.OpenFlag_OPEN_FLAG_WRITE_ONLY | wire.OpenFlag_OPEN_FLAG_CREATE
	if truncate {
		flags |= wire.OpenFlag_OPEN_FLAG_TRUNC
	}
	f, err := cl.Open(path, flags, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, 0)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
