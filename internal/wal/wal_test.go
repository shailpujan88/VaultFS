package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yourusername/vaultfs/internal/store"
)

func TestAppendPutAndReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := w.AppendPut("alpha", []byte("one")); err != nil {
		t.Fatalf("AppendPut: %v", err)
	}
	if err := w.AppendDelete("alpha"); err != nil {
		t.Fatalf("AppendDelete: %v", err)
	}
	if err := w.AppendPut("beta", []byte("two")); err != nil {
		t.Fatalf("AppendPut: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var ops []Entry
	if err := Replay(dir, func(e Entry) error {
		ops = append(ops, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(ops) != 3 {
		t.Fatalf("got %d entries, want 3", len(ops))
	}
	if ops[0].OperationType != OperationPut || ops[0].Key != "alpha" {
		t.Fatalf("first entry: %+v", ops[0])
	}
	if ops[0].Timestamp.IsZero() {
		t.Fatal("expected timestamp on first entry")
	}
	if ops[1].OperationType != OperationDelete || ops[1].Key != "alpha" {
		t.Fatalf("second entry: %+v", ops[1])
	}
	if ops[2].OperationType != OperationPut || ops[2].Key != "beta" {
		t.Fatalf("third entry: %+v", ops[2])
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	expected := make(map[string][]byte, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		val := []byte(fmt.Sprintf("value-%03d", i))
		expected[key] = val
		if err := w.AppendPut(key, val); err != nil {
			t.Fatalf("AppendPut %s: %v", key, err)
		}
	}

	// Simulate crash: durable WAL on disk, in-memory store never written.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st := store.New()
	t.Cleanup(func() { st.Close() })

	if err := Replay(dir, func(e Entry) error {
		switch e.OperationType {
		case OperationPut:
			st.Put(e.Key, e.Value)
		case OperationDelete:
			_ = st.Delete(e.Key)
		default:
			return fmt.Errorf("unknown operation: %d", e.OperationType)
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	for key, want := range expected {
		got, err := st.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != string(want) {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}

	if st.Len() != 100 {
		t.Fatalf("Len() = %d, want 100", st.Len())
	}
}

func TestLogRotation(t *testing.T) {
	dir := t.TempDir()

	const maxSize = 512
	w, err := Open(dir, WithMaxSegmentSize(maxSize))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	large := make([]byte, 200)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("rotate-key-%02d", i)
		if err := w.AppendPut(key, large); err != nil {
			t.Fatalf("AppendPut: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	count, err := w.SegmentCount()
	if err != nil {
		t.Fatalf("SegmentCount: %v", err)
	}
	if count < 2 {
		t.Fatalf("SegmentCount = %d, want at least 2 after rotation", count)
	}

	st := store.New()
	t.Cleanup(func() { st.Close() })

	if err := Replay(dir, func(e Entry) error {
		if e.OperationType == OperationPut {
			st.Put(e.Key, e.Value)
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if st.Len() != 30 {
		t.Fatalf("Len() = %d, want 30", st.Len())
	}
}

func TestReplayEmptyDir(t *testing.T) {
	dir := t.TempDir()
	called := false
	if err := Replay(dir, func(e Entry) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if called {
		t.Fatal("apply should not run for empty wal directory")
	}
}

func TestReplayDeleteRemovesKey(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.AppendPut("gone", []byte("x")); err != nil {
		t.Fatalf("AppendPut: %v", err)
	}
	if err := w.AppendDelete("gone"); err != nil {
		t.Fatalf("AppendDelete: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	st := store.New()
	t.Cleanup(func() { st.Close() })

	if err := Replay(dir, func(e Entry) error {
		switch e.OperationType {
		case OperationPut:
			st.Put(e.Key, e.Value)
		case OperationDelete:
			if err := st.Delete(e.Key); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if _, err := st.Get("gone"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get(gone): got %v, want ErrNotFound", err)
	}
}

func TestSegmentFilesExist(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, WithMaxSegmentSize(128))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := w.AppendPut(fmt.Sprintf("k%d", i), []byte("payload")); err != nil {
			t.Fatalf("AppendPut: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one segment file")
	}
}
