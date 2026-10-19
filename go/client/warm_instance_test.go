package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

// restartFake replaces the server instance: warm ids restart and every
// connection drops.
func restartFake(s *fakeServer, instance uint64) {
	s.mu.Lock()
	s.instance = instance
	s.nt.nextWarm, s.nt.warmConn = 0, map[uint64]int{}
	s.mu.Unlock()
	s.dropAll()
}

func TestWaitWarmRejectsResultOfReplacedServer(t *testing.T) {
	s := newFakeServer(t, 0)
	s.instance = 11
	cl := dial(t, s, 1)
	id, err := cl.WarmOpen("/a")
	if err != nil {
		t.Fatal(err)
	}
	s.warmDone(id)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = cl.WaitWarm(ctx, id); err != nil {
		t.Fatalf("WaitWarm on the first instance: %v", err)
	}

	restartFake(s, 22)
	// A call dials again and meets the new instance.
	for i := 0; i < 10 && cl.Ping() != nil; i++ {
	}
	id2, err := cl.WarmOpen("/b")
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Fatalf("new server warm id = %d, want the reused %d", id2, id)
	}
	short, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer stop()
	if err = cl.WaitWarm(short, id2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitWarm took the old completion: %v", err)
	}
	s.warmDone(id2)
	if err = cl.WaitWarm(ctx, id2); err != nil {
		t.Fatalf("WaitWarm on the new instance: %v", err)
	}
}

func TestWaitWarmFailsWaiterOfReplacedServer(t *testing.T) {
	s := newFakeServer(t, 0)
	s.instance = 11
	cl := dial(t, s, 1)
	id, err := cl.WarmOpen("/a")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- cl.WaitWarm(context.Background(), id) }()
	time.Sleep(50 * time.Millisecond)
	restartFake(s, 22)
	for i := 0; i < 10 && cl.Ping() != nil; i++ {
	}
	select {
	case err := <-got:
		if !errors.Is(err, ErrServerRestarted) {
			t.Fatalf("WaitWarm = %v, want ErrServerRestarted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter of the old instance still blocked")
	}
}

func TestWaitWarmKeepsResultWithoutInstanceID(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	id, err := cl.WarmOpen("/a")
	if err != nil {
		t.Fatal(err)
	}
	s.warmDone(id)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = cl.WaitWarm(ctx, id); err != nil {
		t.Fatal(err)
	}
	s.dropAll()
	for i := 0; i < 10 && cl.Ping() != nil; i++ {
	}
	if err = cl.WaitWarm(ctx, id); err != nil {
		t.Fatalf("result lost on a server without instance_id: %v", err)
	}
}

func TestWaitWarmRejectsResultOfServerThatGainedInstanceID(t *testing.T) {
	s := newFakeServer(t, 0)
	cl := dial(t, s, 1)
	id, err := cl.WarmOpen("/a")
	if err != nil {
		t.Fatal(err)
	}
	s.warmDone(id)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = cl.WaitWarm(ctx, id); err != nil {
		t.Fatalf("WaitWarm on the server without an id: %v", err)
	}
	pending, err := cl.WarmOpen("/b")
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() { got <- cl.WaitWarm(context.Background(), pending) }()
	time.Sleep(50 * time.Millisecond)

	restartFake(s, 22)
	for i := 0; i < 10 && cl.Ping() != nil; i++ {
	}
	select {
	case err = <-got:
		if !errors.Is(err, ErrServerRestarted) {
			t.Fatalf("WaitWarm = %v, want ErrServerRestarted", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter of the old generation still blocked")
	}
	id2, err := cl.WarmOpen("/c")
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id {
		t.Fatalf("new server warm id = %d, want the reused %d", id2, id)
	}
	short, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer stop()
	if err = cl.WaitWarm(short, id2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitWarm took the old completion: %v", err)
	}
	s.warmDone(id2)
	if err = cl.WaitWarm(ctx, id2); err != nil {
		t.Fatalf("WaitWarm on the new instance: %v", err)
	}
}
