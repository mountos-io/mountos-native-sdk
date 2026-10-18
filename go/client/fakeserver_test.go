package client

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mountos-io/mountos-native-sdk/go/frame"
	"github.com/mountos-io/mountos-native-sdk/go/wire"
	"google.golang.org/protobuf/proto"
)

// fakeServer speaks the frame protocol over a unix socket with an in-memory file store.
type fakeServer struct {
	t          *testing.T
	ln         net.Listener
	path       string
	maxPayload uint32 // advertised in Hello
	lim        frame.Limits

	mu         sync.Mutex
	files      map[string][]byte
	nextHandle uint64
	handles    map[uint64]hent
	violations int // handle used on a connection that did not open it
	conns      []net.Conn
	maxSeen    int                 // largest request payload
	writes     int                 // WriteAt frames received
	links      map[string]string   // symlink path -> target
	dirs       map[string][]string // directory path -> entry names
	times      map[string][2]int64 // path -> atime, mtime ns
	dirPos     map[uint64]int      // Readdir position per dir handle
	nlink      map[string]uint32

	wmu        sync.Mutex // serializes frame writes, since watch events are pushed
	connByIdx  map[int]net.Conn
	statSizes  []int // paths per StatBatch request
	readSizes  []int // ranges per ReadBatch request
	writeSizes []int // ranges per WriteBatch request

	nt fakeNotify // warm ids, watch ids and notice state

	caps       []string                 // capabilities answered in Hello
	helloCaps  []string                 // capabilities the last Hello asked for
	stall      atomic.Int32             // Ping requests to leave unanswered
	delays     map[string]time.Duration // Stat delay per path
	slowSeen   atomic.Int32             // delayed Stat requests received
	batchDelay time.Duration            // pause inside each batch request
	inflight   atomic.Int32             // batch requests being served
	peak       atomic.Int32             // most batch requests served at once
	noInode    bool                     // Fstat reports no inode

	// When barrierN > 0, ReadAt blocks until barrierN requests are in flight at once.
	barrierN int
	arrived  int
	release  chan struct{}
}

type hent struct {
	conn int
	path string
}

func newFakeServer(t *testing.T, maxPayload uint32) *fakeServer {
	t.Helper()
	dir, err := os.MkdirTemp("", "mfs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &fakeServer{
		t: t, path: filepath.Join(dir, "api.sock"), maxPayload: maxPayload,
		lim:   frame.Limits{MaxMetaBytes: maxMetaBytes, MaxPayloadBytes: maxPayloadBytes},
		files: map[string][]byte{}, handles: map[uint64]hent{},
		links: map[string]string{}, dirs: map[string][]string{}, times: map[string][2]int64{},
		dirPos: map[uint64]int{}, nlink: map[string]uint32{},
		connByIdx: map[int]net.Conn{},
	}
	s.ln, err = net.Listen("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.ln.Close(); s.dropAll() })
	go s.accept()
	return s
}

func (s *fakeServer) accept() {
	for i := 0; ; i++ {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.connByIdx[i] = c
		s.mu.Unlock()
		go s.serve(c, i)
	}
}

func (s *fakeServer) dropAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
}

func (s *fakeServer) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *fakeServer) serve(c net.Conn, idx int) {
	defer c.Close()
	first := true
	for {
		f, err := frame.Read(c, s.lim)
		if err != nil {
			return
		}
		if first && f.Method != mHello {
			s.reply(c, f, nil, nil, &wire.WireError{Code: "EINVAL", Message: "hello required"})
			return
		}
		first = false
		resp, data, werr := s.handle(f, idx)
		s.reply(c, f, resp, data, werr)
		if f.Method == mWatch && werr == nil {
			s.afterWatch(idx, f, resp)
		}
	}
}

func (s *fakeServer) reply(c net.Conn, f frame.Frame, resp proto.Message, data []byte, werr *wire.WireError) {
	out := frame.Frame{Flags: frame.FlagResponse, Method: f.Method, RequestID: f.RequestID, Payload: data}
	m := resp
	if werr != nil {
		out.Flags |= frame.FlagError
		m = werr
	}
	if m != nil {
		out.Meta, _ = proto.Marshal(m)
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	frame.Write(c, out, s.lim)
}

func errno(code string) *wire.WireError { return &wire.WireError{Code: code, Message: "fake " + code} }

// own returns the handle entry when it belongs to connection idx.
func (s *fakeServer) own(h uint64, idx int) (hent, *wire.WireError) {
	e, ok := s.handles[h]
	if !ok {
		return hent{}, errno("EBADF")
	}
	if e.conn != idx {
		s.violations++
		return hent{}, errno("EBADF")
	}
	return e, nil
}

func (s *fakeServer) waitBarrier() {
	s.mu.Lock()
	if s.barrierN == 0 {
		s.mu.Unlock()
		return
	}
	s.arrived++
	if s.arrived == s.barrierN {
		close(s.release)
	}
	rel := s.release
	s.mu.Unlock()
	select {
	case <-rel:
	case <-time.After(3 * time.Second):
	}
}

func (s *fakeServer) handle(f frame.Frame, idx int) (proto.Message, []byte, *wire.WireError) {
	switch f.Method {
	case mHello:
		var r wire.HelloRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		s.helloCaps = r.Capabilities
		caps := s.caps
		s.mu.Unlock()
		return &wire.HelloResponse{Version: 1, MaxPayloadBytes: s.maxPayload, MaxInflight: 1, Capabilities: caps}, nil, nil
	case mPing:
		if s.stall.Add(-1) >= 0 {
			time.Sleep(time.Second)
			return &wire.Empty{}, nil, nil
		}
		s.stall.Store(0)
		s.mu.Lock()
		n := s.nt.pingNotice
		s.mu.Unlock()
		for i := 0; i < n; i++ {
			s.notify(idx, &wire.Notice{Kind: wire.NoticeKind_NOTICE_KIND_CLOSING})
		}
		return &wire.Empty{}, nil, nil
	case mStat:
		var r wire.StatRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		pause := s.delays[r.Path]
		s.mu.Unlock()
		if pause > 0 {
			s.slowSeen.Add(1)
			time.Sleep(pause)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		d, ok := s.files[r.Path]
		if !ok {
			return nil, nil, errno("ENOENT")
		}
		return &wire.StatResponse{Info: s.info(r.Path, int64(len(d)))}, nil, nil
	case mOpen:
		var r wire.OpenRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.files[r.Path]; !ok {
			if r.Flags&uint32(wire.OpenFlag_OPEN_FLAG_CREATE) == 0 {
				return nil, nil, errno("ENOENT")
			}
			s.files[r.Path] = nil
		}
		if r.Flags&uint32(wire.OpenFlag_OPEN_FLAG_TRUNC) != 0 {
			s.files[r.Path] = nil
		}
		s.nextHandle++
		s.handles[s.nextHandle] = hent{conn: idx, path: r.Path}
		return &wire.OpenResponse{Handle: s.nextHandle}, nil, nil
	case mClose:
		var r wire.CloseRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, e := s.own(r.Handle, idx); e != nil {
			return nil, nil, e
		}
		delete(s.handles, r.Handle)
		return &wire.Empty{}, nil, nil
	case mFstat:
		var r wire.FstatRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		defer s.mu.Unlock()
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e
		}
		info := &wire.FileInfo{Name: h.path, Size: int64(len(s.files[h.path]))}
		if !s.noInode {
			info.Inode = uint64(len(h.path)) + 1000
		}
		return &wire.FstatResponse{Info: info}, nil, nil
	case mReadAt:
		var r wire.ReadAtRequest
		proto.Unmarshal(f.Meta, &r)
		s.waitBarrier()
		s.mu.Lock()
		defer s.mu.Unlock()
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e
		}
		d := s.files[h.path]
		if r.Offset >= int64(len(d)) {
			return &wire.ReadAtResponse{}, nil, nil
		}
		out := d[r.Offset:min(int64(len(d)), r.Offset+int64(r.Length))]
		return &wire.ReadAtResponse{Count: uint32(len(out))}, out, nil
	case mWriteAt:
		var r wire.WriteAtRequest
		proto.Unmarshal(f.Meta, &r)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.writes++
		s.maxSeen = max(s.maxSeen, len(f.Payload))
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e
		}
		d := s.files[h.path]
		if end := int(r.Offset) + len(f.Payload); end > len(d) {
			d = append(d, make([]byte, end-len(d))...)
		}
		copy(d[r.Offset:], f.Payload)
		s.files[h.path] = d
		return &wire.WriteAtResponse{Count: uint32(len(f.Payload))}, nil, nil
	}
	if f.Method == mStatBatch || f.Method == mReadBatch || f.Method == mWriteBatch {
		n := s.inflight.Add(1)
		defer s.inflight.Add(-1)
		for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
		}
		time.Sleep(s.batchDelay)
	}
	if resp, data, werr, ok := s.handleBulk(f, idx); ok {
		return resp, data, werr
	}
	if resp, werr, ok := s.handleNotify(f, idx); ok {
		return resp, nil, werr
	}
	return s.handleNS(f, idx)
}

// info builds attributes for path, including recorded times and link count.
func (s *fakeServer) info(path string, size int64) *wire.FileInfo {
	t := s.times[path]
	n := s.nlink[path]
	if n == 0 {
		n = 1
	}
	return &wire.FileInfo{Name: path, Size: size, AtimeNs: t[0], MtimeNs: t[1], Nlink: n}
}

// handleNS serves the namespace methods: links, times, statfs, directories, remove-all.
func (s *fakeServer) handleNS(f frame.Frame, idx int) (proto.Message, []byte, *wire.WireError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch f.Method {
	case mLstat:
		var r wire.LstatRequest
		proto.Unmarshal(f.Meta, &r)
		if t, ok := s.links[r.Path]; ok {
			return &wire.LstatResponse{Info: &wire.FileInfo{Name: r.Path, Size: int64(len(t)), Type: wire.FileType_FILE_TYPE_SYMLINK}}, nil, nil
		}
		d, ok := s.files[r.Path]
		if !ok {
			return nil, nil, errno("ENOENT")
		}
		return &wire.LstatResponse{Info: s.info(r.Path, int64(len(d)))}, nil, nil
	case mSymlink:
		var r wire.SymlinkRequest
		proto.Unmarshal(f.Meta, &r)
		if _, ok := s.links[r.LinkPath]; ok {
			return nil, nil, errno("EEXIST")
		}
		s.links[r.LinkPath] = r.Target
		return &wire.Empty{}, nil, nil
	case mReadlink:
		var r wire.ReadlinkRequest
		proto.Unmarshal(f.Meta, &r)
		t, ok := s.links[r.Path]
		if !ok {
			if _, isFile := s.files[r.Path]; isFile {
				return nil, nil, errno("EINVAL")
			}
			return nil, nil, errno("ENOENT")
		}
		return &wire.ReadlinkResponse{Target: t}, nil, nil
	case mLink:
		var r wire.LinkRequest
		proto.Unmarshal(f.Meta, &r)
		d, ok := s.files[r.Existing]
		if !ok {
			return nil, nil, errno("ENOENT")
		}
		if _, ok := s.files[r.NewPath]; ok {
			return nil, nil, errno("EEXIST")
		}
		s.files[r.NewPath] = d
		s.nlink[r.Existing]++
		if s.nlink[r.Existing] < 2 {
			s.nlink[r.Existing] = 2
		}
		s.nlink[r.NewPath] = s.nlink[r.Existing]
		return &wire.Empty{}, nil, nil
	case mUtimens:
		var r wire.UtimensRequest
		proto.Unmarshal(f.Meta, &r)
		if _, ok := s.files[r.Path]; !ok {
			return nil, nil, errno("ENOENT")
		}
		t := s.times[r.Path]
		if r.SetAtime {
			t[0] = r.AtimeNs
		}
		if r.SetMtime {
			t[1] = r.MtimeNs
		}
		s.times[r.Path] = t
		return &wire.Empty{}, nil, nil
	case mStatfs:
		return &wire.StatfsResponse{BlockSize: 4096, Blocks: 1000, BlocksFree: 600, BlocksAvail: 500, Files: 100, FilesFree: 90, NameMax: 255}, nil, nil
	case mOpendir:
		var r wire.OpendirRequest
		proto.Unmarshal(f.Meta, &r)
		if _, ok := s.dirs[r.Path]; !ok {
			return nil, nil, errno("ENOENT")
		}
		s.nextHandle++
		s.handles[s.nextHandle] = hent{conn: idx, path: r.Path}
		s.dirPos[s.nextHandle] = 0
		return &wire.OpendirResponse{Handle: s.nextHandle}, nil, nil
	case mReaddir:
		var r wire.ReaddirRequest
		proto.Unmarshal(f.Meta, &r)
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e
		}
		names := s.dirs[h.path]
		pos := s.dirPos[r.Handle]
		end := min(len(names), pos+int(r.MaxEntries))
		out := &wire.ReaddirResponse{Eof: end == len(names)}
		for _, n := range names[pos:end] {
			out.Entries = append(out.Entries, &wire.DirEntry{Name: n})
		}
		s.dirPos[r.Handle] = end
		return out, nil, nil
	case mClosedir:
		var r wire.ClosedirRequest
		proto.Unmarshal(f.Meta, &r)
		if _, e := s.own(r.Handle, idx); e != nil {
			return nil, nil, e
		}
		delete(s.handles, r.Handle)
		delete(s.dirPos, r.Handle)
		return &wire.Empty{}, nil, nil
	case mLs:
		var r wire.LsRequest
		proto.Unmarshal(f.Meta, &r)
		names, ok := s.dirs[r.Path]
		if !ok {
			return nil, nil, errno("ENOENT")
		}
		pos := 0
		if r.Cursor != "" {
			if _, err := fmt.Sscanf(r.Cursor, "c%d", &pos); err != nil {
				return nil, nil, errno("EINVAL")
			}
		}
		end := min(len(names), pos+int(r.MaxEntries))
		out := &wire.LsResponse{}
		for _, n := range names[pos:end] {
			e := &wire.DirEntry{Name: n, Info: &wire.FileInfo{Name: n, Size: int64(len(s.files[r.Path+"/"+n]))}}
			if _, isDir := s.dirs[r.Path+"/"+n]; isDir {
				e.Type, e.Info.Type = wire.FileType_FILE_TYPE_DIRECTORY, wire.FileType_FILE_TYPE_DIRECTORY
			}
			out.Entries = append(out.Entries, e)
		}
		if end < len(names) {
			out.NextCursor = fmt.Sprintf("c%d", end)
		}
		return out, nil, nil
	case mRemoveAll:
		var r wire.RemoveAllRequest
		proto.Unmarshal(f.Meta, &r)
		var n uint64
		for p := range s.files {
			if p == r.Path || strings.HasPrefix(p, r.Path+"/") {
				delete(s.files, p)
				n++
			}
		}
		if _, ok := s.dirs[r.Path]; ok {
			delete(s.dirs, r.Path)
			n++
		}
		if n == 0 {
			return nil, nil, errno("ENOENT")
		}
		return &wire.RemoveAllResponse{Removed: n}, nil, nil
	}
	return nil, nil, errno("ENOSYS")
}

// limit is the largest payload the server accepts per request.
func (s *fakeServer) limit() int {
	if s.maxPayload > 0 {
		return int(s.maxPayload)
	}
	return maxPayloadBytes
}

// badOffset makes a batch item fail on the server with EIO.
const badOffset = 1 << 40

// handleBulk serves the batch and whole-file methods. ok is false for other methods.
func (s *fakeServer) handleBulk(f frame.Frame, idx int) (resp proto.Message, data []byte, werr *wire.WireError, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch f.Method {
	case mStatBatch:
		var r wire.StatBatchRequest
		proto.Unmarshal(f.Meta, &r)
		s.statSizes = append(s.statSizes, len(r.Paths))
		if len(r.Paths) > 4096 {
			return nil, nil, errno("EINVAL"), true
		}
		out := &wire.StatBatchResponse{}
		for i, p := range r.Paths {
			d, found := s.files[p]
			if !found {
				out.Infos = append(out.Infos, &wire.FileInfo{})
				out.Errors = append(out.Errors, &wire.BatchItemError{Index: uint32(i), Code: "ENOENT", Message: "no such file"})
				continue
			}
			out.Infos = append(out.Infos, s.info(p, int64(len(d))))
		}
		return out, nil, nil, true
	case mReadBatch:
		var r wire.ReadBatchRequest
		proto.Unmarshal(f.Meta, &r)
		s.readSizes = append(s.readSizes, len(r.Ranges))
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e, true
		}
		total := 0
		for _, rg := range r.Ranges {
			total += int(rg.Length)
		}
		if len(r.Ranges) > 4096 || total > s.limit() {
			return nil, nil, errno("EINVAL"), true
		}
		d := s.files[h.path]
		out := &wire.ReadBatchResponse{}
		var payload []byte
		for i, rg := range r.Ranges {
			if rg.Offset < 0 || rg.Offset == badOffset {
				out.Counts = append(out.Counts, 0)
				out.Errors = append(out.Errors, &wire.BatchItemError{Index: uint32(i), Code: "EIO", Message: "bad offset"})
				continue
			}
			var part []byte
			if rg.Offset < int64(len(d)) {
				part = d[rg.Offset:min(int64(len(d)), rg.Offset+int64(rg.Length))]
			}
			out.Counts = append(out.Counts, uint32(len(part)))
			payload = append(payload, part...)
		}
		return out, payload, nil, true
	case mWriteBatch:
		var r wire.WriteBatchRequest
		proto.Unmarshal(f.Meta, &r)
		s.writeSizes = append(s.writeSizes, len(r.Ranges))
		h, e := s.own(r.Handle, idx)
		if e != nil {
			return nil, nil, e, true
		}
		total := 0
		for _, rg := range r.Ranges {
			total += int(rg.Length)
		}
		if len(r.Ranges) > 4096 || total != len(f.Payload) {
			return nil, nil, errno("EINVAL"), true
		}
		d := s.files[h.path]
		out := &wire.WriteBatchResponse{}
		pos := 0
		for i, rg := range r.Ranges {
			part := f.Payload[pos : pos+int(rg.Length)]
			pos += int(rg.Length)
			if rg.Offset < 0 || rg.Offset == badOffset {
				out.Counts = append(out.Counts, 0)
				out.Errors = append(out.Errors, &wire.BatchItemError{Index: uint32(i), Code: "EIO", Message: "bad offset"})
				continue
			}
			if end := int(rg.Offset) + len(part); end > len(d) {
				d = append(d, make([]byte, end-len(d))...)
			}
			copy(d[rg.Offset:], part)
			out.Counts = append(out.Counts, uint32(len(part)))
		}
		s.files[h.path] = d
		return out, nil, nil, true
	case mReadFile:
		var r wire.ReadFileRequest
		proto.Unmarshal(f.Meta, &r)
		d, found := s.files[r.Path]
		if !found {
			return nil, nil, errno("ENOENT"), true
		}
		n := s.limit()
		if r.MaxBytes > 0 && int(r.MaxBytes) < n {
			n = int(r.MaxBytes)
		}
		out := append([]byte(nil), d[:min(len(d), n)]...)
		return &wire.ReadFileResponse{Count: uint32(len(out))}, out, nil, true
	case mWriteFile:
		var r wire.WriteFileRequest
		proto.Unmarshal(f.Meta, &r)
		d := s.files[r.Path]
		if r.Truncate {
			d = nil
		}
		if len(f.Payload) > len(d) {
			d = append(d, make([]byte, len(f.Payload)-len(d))...)
		}
		copy(d, f.Payload)
		s.files[r.Path] = d
		return &wire.WriteFileResponse{Count: uint32(len(f.Payload))}, nil, nil, true
	}
	return nil, nil, nil, false
}
