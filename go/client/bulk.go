package client

import (
	"fmt"
	"io"

	"github.com/mountos-io/mountos-native-sdk/go/wire"
)

// maxBatchItems is the most paths or ranges the server accepts in one batch request.
const maxBatchItems = 4096

// StatResult is the outcome for one path of StatBatch. Info is nil when Err is set.
type StatResult struct {
	Info *wire.FileInfo
	Err  error
}

// Range is a byte range of a file for ReadBatch.
type Range struct {
	Offset int64
	Length uint32
}

// ReadResult is the outcome for one Range. Data is shorter than the range
// length when the range reaches past end of file.
type ReadResult struct {
	Data []byte
	Err  error
}

// WriteItem is data to write at Offset for WriteBatch.
type WriteItem struct {
	Offset int64
	Data   []byte
}

// WriteResult is the outcome for one WriteItem. N is the number of bytes written.
type WriteResult struct {
	N   int
	Err error
}

// itemErrors maps the batch error list to errors by position. Every index must
// be below n.
func itemErrors(list []*wire.BatchItemError, n int, op string) (map[int]error, error) {
	if len(list) == 0 {
		return nil, nil
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

// StatBatch stats every path and returns one result per path, in order. A
// failed path sets Err for that entry only. Large inputs are split into
// several requests.
func (cl *Client) StatBatch(paths []string) ([]StatResult, error) {
	out := make([]StatResult, 0, len(paths))
	for start := 0; start < len(paths); start += maxBatchItems {
		part := paths[start:min(len(paths), start+maxBatchItems)]
		var r wire.StatBatchResponse
		if err := cl.run(mStatBatch, &wire.StatBatchRequest{Paths: part}, &r); err != nil {
			return nil, err
		}
		if len(r.GetInfos()) != len(part) {
			return nil, &ProtocolError{Op: "stat batch", Err: fmt.Errorf("%d infos for %d paths", len(r.GetInfos()), len(part))}
		}
		errs, err := itemErrors(r.GetErrors(), len(part), "stat batch")
		if err != nil {
			return nil, err
		}
		for i, info := range r.GetInfos() {
			if e, ok := errs[i]; ok {
				out = append(out, StatResult{Err: e})
			} else {
				out = append(out, StatResult{Info: info})
			}
		}
	}
	return out, nil
}

// piece is a slice of one caller item, small enough to travel in one frame.
type piece struct {
	owner  int
	offset int64
	length int
	src    int // offset into the owner's Data (writes)
}

// pieces splits items of the given lengths so that no piece exceeds max bytes.
// A zero-length item still yields one piece so it gets a result.
func pieces(offsets []int64, lengths []int, max int) []piece {
	var out []piece
	for i, n := range lengths {
		if n == 0 {
			out = append(out, piece{owner: i, offset: offsets[i]})
			continue
		}
		for done := 0; done < n; {
			l := min(max, n-done)
			out = append(out, piece{owner: i, offset: offsets[i] + int64(done), length: l, src: done})
			done += l
		}
	}
	return out
}

// groups cuts pieces into requests of at most maxBatchItems pieces and max total bytes.
func groups(ps []piece, max int) [][]piece {
	var out [][]piece
	for len(ps) > 0 {
		n, bytes := 0, 0
		for n < len(ps) && n < maxBatchItems && bytes+ps[n].length <= max {
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

// ReadBatch reads several ranges of the file in as few requests as the server
// limits allow. Results are in range order. A range past end of file returns
// fewer bytes than its length. The File position is not used or changed.
func (f *File) ReadBatch(ranges []Range) ([]ReadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offsets := make([]int64, len(ranges))
	lengths := make([]int, len(ranges))
	for i, r := range ranges {
		if r.Offset < 0 {
			return nil, fmt.Errorf("mountos: negative offset in range %d", i)
		}
		offsets[i], lengths[i] = r.Offset, int(r.Length)
	}
	res := make([]ReadResult, len(ranges))
	for _, g := range groups(pieces(offsets, lengths, f.chunk()), f.chunk()) {
		var r wire.ReadBatchResponse
		data, err := f.do(mReadBatch, &wire.ReadBatchRequest{Handle: f.handle, Ranges: wireRanges(g)}, &r, nil)
		if err != nil {
			return nil, err
		}
		if len(r.GetCounts()) != len(g) {
			return nil, &ProtocolError{Op: "read batch", Err: fmt.Errorf("%d counts for %d ranges", len(r.GetCounts()), len(g))}
		}
		errs, err := itemErrors(r.GetErrors(), len(g), "read batch")
		if err != nil {
			return nil, err
		}
		pos := 0
		for i, p := range g {
			n := int(r.GetCounts()[i])
			if n > p.length || pos+n > len(data) {
				return nil, &ProtocolError{Op: "read batch", Err: fmt.Errorf("count %d for range %d of %d bytes, payload %d", n, i, p.length, len(data))}
			}
			out := &res[p.owner]
			out.Data = append(out.Data, data[pos:pos+n]...)
			pos += n
			if e, ok := errs[i]; ok && out.Err == nil {
				out.Err = e
			}
		}
		if pos != len(data) {
			return nil, &ProtocolError{Op: "read batch", Err: fmt.Errorf("payload %d bytes, counts sum %d", len(data), pos)}
		}
	}
	return res, nil
}

// WriteBatch writes several items in as few requests as the server limits
// allow. Results are in item order. The File position is not used or changed.
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
	res := make([]WriteResult, len(items))
	for _, g := range groups(pieces(offsets, lengths, f.chunk()), f.chunk()) {
		var payload []byte
		for _, p := range g {
			payload = append(payload, items[p.owner].Data[p.src:p.src+p.length]...)
		}
		var r wire.WriteBatchResponse
		if _, err := f.do(mWriteBatch, &wire.WriteBatchRequest{Handle: f.handle, Ranges: wireRanges(g)}, &r, payload); err != nil {
			return nil, err
		}
		if len(r.GetCounts()) != len(g) {
			return nil, &ProtocolError{Op: "write batch", Err: fmt.Errorf("%d counts for %d ranges", len(r.GetCounts()), len(g))}
		}
		errs, err := itemErrors(r.GetErrors(), len(g), "write batch")
		if err != nil {
			return nil, err
		}
		for i, p := range g {
			n := int(r.GetCounts()[i])
			if n > p.length {
				return nil, &ProtocolError{Op: "write batch", Err: fmt.Errorf("count %d exceeds sent %d", n, p.length)}
			}
			out := &res[p.owner]
			out.N += n
			if e, ok := errs[i]; ok && out.Err == nil {
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
// chunks, so the result is complete but not an atomic snapshot.
func (cl *Client) ReadFile(path string) ([]byte, error) {
	if cl.isClosed() {
		return nil, ErrClientClosed
	}
	c := cl.acquire()
	if err := c.ensure(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	limit := c.maxData
	var r wire.ReadFileResponse
	data, err := c.call(mReadFile, &wire.ReadFileRequest{Path: path, MaxBytes: uint64(limit)}, &r, nil)
	c.mu.Unlock()
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
	defer f.Close()
	buf := make([]byte, limit)
	for {
		n, err := f.ReadAt(buf, int64(len(data)))
		data = append(data, buf[:n]...)
		if err == io.EOF {
			return data, nil
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
	c := cl.acquire()
	if err := c.ensure(); err != nil {
		c.mu.Unlock()
		return err
	}
	if len(data) <= c.maxData {
		var r wire.WriteFileResponse
		_, err := c.call(mWriteFile, &wire.WriteFileRequest{Path: path, Truncate: truncate}, &r, data)
		c.mu.Unlock()
		if err != nil {
			return err
		}
		if int(r.GetCount()) != len(data) {
			return io.ErrShortWrite
		}
		return nil
	}
	c.mu.Unlock()
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
