package wal

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxSegmentSize = 10 * 1024 * 1024 // 10MB
	segmentPrefix         = "segment-"
	segmentSuffix         = ".wal"
	walSubdir             = "wal"
)

// OperationType identifies a WAL record.
type OperationType int

const (
	OperationPut OperationType = iota + 1
	OperationDelete
)

// Entry is a single write-ahead log record.
type Entry struct {
	OperationType OperationType
	Key           string
	Value         []byte
	Timestamp     time.Time
}

// WAL appends durable gob-encoded records to segmented binary log files.
type WAL struct {
	mu             sync.Mutex
	dir            string
	walDir         string
	segmentID      int
	file           *os.File
	encoder        *gob.Encoder
	maxSegmentSize int64
}

// Option configures a WAL.
type Option func(*WAL)

// WithMaxSegmentSize sets the segment size threshold that triggers rotation.
func WithMaxSegmentSize(size int64) Option {
	return func(w *WAL) {
		if size > 0 {
			w.maxSegmentSize = size
		}
	}
}

// Open creates or resumes the active segment under dir/wal/.
func Open(dir string, opts ...Option) (*WAL, error) {
	walDir := filepath.Join(dir, walSubdir)
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}

	w := &WAL{
		dir:            dir,
		walDir:         walDir,
		maxSegmentSize: defaultMaxSegmentSize,
	}

	for _, opt := range opts {
		opt(w)
	}

	segmentID, err := w.latestSegmentID()
	if err != nil {
		return nil, err
	}

	if segmentID == 0 {
		segmentID = 1
	} else if size, err := segmentSize(walDir, segmentID); err != nil {
		return nil, err
	} else if size >= w.maxSegmentSize {
		segmentID++
	}

	if err := w.openSegment(segmentID); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) latestSegmentID() (int, error) {
	entries, err := os.ReadDir(w.walDir)
	if err != nil {
		return 0, fmt.Errorf("read wal directory: %w", err)
	}

	maxID := 0
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		id, ok := parseSegmentName(ent.Name())
		if !ok {
			continue
		}
		if id > maxID {
			maxID = id
		}
	}
	return maxID, nil
}

func parseSegmentName(name string) (int, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	id, err := strconv.Atoi(raw)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

func segmentPath(walDir string, id int) string {
	return filepath.Join(walDir, fmt.Sprintf("%s%06d%s", segmentPrefix, id, segmentSuffix))
}

func segmentSize(walDir string, id int) (int64, error) {
	info, err := os.Stat(segmentPath(walDir, id))
	if err != nil {
		return 0, fmt.Errorf("stat segment: %w", err)
	}
	return info.Size(), nil
}

func (w *WAL) openSegment(id int) error {
	path := segmentPath(w.walDir, id)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open wal segment: %w", err)
	}

	w.segmentID = id
	w.file = f
	w.encoder = gob.NewEncoder(f)
	return nil
}

// AppendPut records a PUT before it is applied to the in-memory store.
func (w *WAL) AppendPut(key string, value []byte) error {
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	return w.Append(Entry{
		OperationType: OperationPut,
		Key:           key,
		Value:         valCopy,
	})
}

// AppendDelete records a DELETE before it is applied to the in-memory store.
func (w *WAL) AppendDelete(key string) error {
	return w.Append(Entry{
		OperationType: OperationDelete,
		Key:           key,
	})
}

// Append writes a log entry to the active segment and fsyncs it.
func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	if err := w.rotateIfNeeded(); err != nil {
		return err
	}

	if err := w.encoder.Encode(entry); err != nil {
		return fmt.Errorf("encode wal entry: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal segment: %w", err)
	}

	return w.rotateIfNeeded()
}

func (w *WAL) rotateIfNeeded() error {
	size, err := segmentSize(w.walDir, w.segmentID)
	if err != nil {
		return err
	}
	if size < w.maxSegmentSize {
		return nil
	}
	return w.rotate()
}

func (w *WAL) rotate() error {
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("close wal segment: %w", err)
		}
		w.file = nil
		w.encoder = nil
	}
	return w.openSegment(w.segmentID + 1)
}

// Replay reads every segment in order and invokes apply for each entry.
func Replay(dir string, apply func(Entry) error) error {
	walDir := filepath.Join(dir, walSubdir)
	segments, err := listSegments(walDir)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return nil
	}

	for _, seg := range segments {
		if err := replaySegment(seg, apply); err != nil {
			return err
		}
	}
	return nil
}

func listSegments(walDir string) ([]string, error) {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read wal directory: %w", err)
	}

	var segments []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if id, ok := parseSegmentName(ent.Name()); ok && id > 0 {
			segments = append(segments, segmentPath(walDir, id))
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segmentIDFromPath(segments[i]) < segmentIDFromPath(segments[j])
	})
	return segments, nil
}

func segmentIDFromPath(path string) int {
	id, _ := parseSegmentName(filepath.Base(path))
	return id
}

func replaySegment(path string, apply func(Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open wal segment %s: %w", path, err)
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	for {
		var entry Entry
		if err := dec.Decode(&entry); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode wal entry from %s: %w", path, err)
		}
		if err := apply(entry); err != nil {
			return err
		}
	}
}

// Close flushes and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.encoder = nil
	return err
}

// ActiveSegment returns the path of the segment currently open for writes.
func (w *WAL) ActiveSegment() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.segmentID == 0 {
		return ""
	}
	return segmentPath(w.walDir, w.segmentID)
}

// SegmentCount returns how many segment files exist on disk.
func (w *WAL) SegmentCount() (int, error) {
	segments, err := listSegments(w.walDir)
	if err != nil {
		return 0, err
	}
	return len(segments), nil
}
