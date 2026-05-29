package store

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(WithPurgeInterval(20 * time.Millisecond))
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := newTestStore(t)

	s.Put("foo", []byte("bar"))
	val, err := s.Get("foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "bar" {
		t.Fatalf("got %q, want %q", val, "bar")
	}

	if err := s.Delete("foo"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get("foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Get("missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	s := newTestStore(t)

	err := s.Delete("missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestPutOverwrites(t *testing.T) {
	s := newTestStore(t)

	s.Put("k", []byte("v1"))
	s.Put("k", []byte("v2"))

	val, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("got %q, want %q", val, "v2")
	}
}

func TestPutReturnsCopy(t *testing.T) {
	s := newTestStore(t)

	original := []byte("mutable")
	s.Put("k", original)
	original[0] = 'X'

	val, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "mutable" {
		t.Fatalf("store should hold a copy; got %q", val)
	}
}

func TestGetReturnsCopy(t *testing.T) {
	s := newTestStore(t)

	s.Put("k", []byte("value"))
	val, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	val[0] = 'X'

	again, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(again) != "value" {
		t.Fatalf("Get should return a copy; got %q", again)
	}
}

func TestTTLExpiresOnGet(t *testing.T) {
	s := newTestStore(t)

	s.PutWithTTL("temp", []byte("gone"), 30*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	_, err := s.Get("temp")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get: got %v, want ErrNotFound after TTL", err)
	}
}

func TestTTLBackgroundPurge(t *testing.T) {
	s := newTestStore(t)

	s.PutWithTTL("a", []byte("1"), 30*time.Millisecond)
	s.PutWithTTL("b", []byte("2"), 30*time.Millisecond)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.Len() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Len() = %d after TTL; want 0", s.Len())
}

func TestPutWithTTLZeroUsesPermanentPut(t *testing.T) {
	s := newTestStore(t)

	s.PutWithTTL("k", []byte("v"), 0)
	time.Sleep(50 * time.Millisecond)

	val, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("got %q, want %q", val, "v")
	}
}

func TestLenIgnoresExpired(t *testing.T) {
	s := newTestStore(t)

	s.PutWithTTL("x", []byte("1"), 30*time.Millisecond)
	s.Put("y", []byte("2"))

	time.Sleep(50 * time.Millisecond)

	if n := s.Len(); n != 1 {
		t.Fatalf("Len() = %d, want 1", n)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 32
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "key"
			for j := 0; j < iterations; j++ {
				s.Put(key, []byte("writer"))
			}
		}(i)

		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = s.Get("key")
			}
		}()

		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.Delete("key")
				s.Put("key", []byte("restore"))
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentTTL(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 16
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "ttl-key"
			for j := 0; j < iterations; j++ {
				s.PutWithTTL(key, []byte("v"), 100*time.Millisecond)
				_, _ = s.Get(key)
			}
		}(i)
	}

	wg.Wait()
}
