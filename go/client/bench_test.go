package client

import (
	"fmt"
	"sync"
	"testing"
)

// These benchmarks measure the client against the in-memory pipeServer, so
// they show client overhead and the effect of the in-flight limit. The fake
// answers instantly on a goroutine per request, and its bookkeeping is part of
// the figure. End-to-end numbers come from the benchmarks in the server
// repository.

// benchCalls runs op b.N times, spread over g goroutines on a pool of one connection.
func benchCalls(b *testing.B, g int, inflight int, op func(cl *Client, worker int) error, setup func(cl *Client, workers int)) {
	s := newPipeServer(b, true, 32)
	cl := dialPipe(b, s, Options{PoolSize: 1, MaxInflight: inflight})
	if setup != nil {
		setup(cl, g)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < g; w++ {
		n := b.N / g
		if w < b.N%g {
			n++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				if err := op(cl, w); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkStat(b *testing.B) {
	for _, g := range []int{1, 8, 32} {
		for _, inflight := range []int{1, 32} {
			b.Run(fmt.Sprintf("goroutines=%d/inflight=%d", g, inflight), func(b *testing.B) {
				benchCalls(b, g, inflight, func(cl *Client, _ int) error {
					_, err := cl.Stat("/bench")
					return err
				}, nil)
			})
		}
	}
}

func BenchmarkReadAt4K(b *testing.B) {
	for _, g := range []int{1, 8, 32} {
		for _, inflight := range []int{1, 32} {
			b.Run(fmt.Sprintf("goroutines=%d/inflight=%d", g, inflight), func(b *testing.B) {
				files := make([]*File, g)
				buf := make([][]byte, g)
				benchCalls(b, g, inflight, func(cl *Client, w int) error {
					_, err := files[w].ReadAt(buf[w], 0)
					return err
				}, func(cl *Client, workers int) {
					for i := range files {
						f, err := cl.Open("/bench", rw, create)
						if err != nil {
							b.Fatal(err)
						}
						files[i], buf[i] = f, make([]byte, 4096)
					}
				})
			})
		}
	}
}
