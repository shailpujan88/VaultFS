package store

import (
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")

const defaultPurgeInterval = time.Second

type entry struct {
	value     []byte
	expiresAt time.Time // zero means the key does not expire
}

// Store is an in-memory key-value store with optional per-key TTL.
type Store struct {
	mu             sync.RWMutex
	data           map[string]entry
	purgeInterval  time.Duration
	stopCh         chan struct{}
	doneCh         chan struct{}
}

// Option configures a Store.
type Option func(*Store)

// WithPurgeInterval sets how often the background goroutine scans for expired keys.
func WithPurgeInterval(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.purgeInterval = d
		}
	}
}

// New returns a Store and starts a background goroutine that purges expired keys.
func New(opts ...Option) *Store {
	s := &Store{
		data:          make(map[string]entry),
		purgeInterval: defaultPurgeInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	go s.expiryLoop()
	return s
}

// Close stops the background expiry goroutine and waits for it to exit.
func (s *Store) Close() {
	close(s.stopCh)
	<-s.doneCh
}

func (s *Store) expiryLoop() {
	defer close(s.doneCh)

	ticker := time.NewTicker(s.purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.purgeExpired()
		}
	}
}

func (s *Store) purgeExpired() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, e := range s.data {
		if expired(e, now) {
			delete(s.data, key)
		}
	}
}

func expired(e entry, now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Get returns the value for key, or ErrNotFound if the key is missing or expired.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	e, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return nil, ErrNotFound
	}
	if expired(e, time.Now()) {
		s.mu.RUnlock()
		s.mu.Lock()
		if e, ok := s.data[key]; ok && expired(e, time.Now()) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	s.mu.RUnlock()
	return out, nil
}

// Put stores value under key with no expiration.
func (s *Store) Put(key string, value []byte) {
	s.put(key, value, 0)
}

// PutWithTTL stores value under key; the entry is removed after ttl elapses.
func (s *Store) PutWithTTL(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		s.Put(key, value)
		return
	}
	s.put(key, value, ttl)
}

func (s *Store) put(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]byte, len(value))
	copy(copied, value)

	e := entry{value: copied}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	s.data[key] = e
}

// Delete removes key from the store.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return ErrNotFound
	}
	delete(s.data, key)
	return nil
}

// Len returns the number of non-expired keys in the store.
func (s *Store) Len() int {
	now := time.Now()

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := 0
	for _, e := range s.data {
		if !expired(e, now) {
			n++
		}
	}
	return n
}
