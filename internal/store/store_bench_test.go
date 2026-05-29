package store

import (
	"fmt"
	"testing"
)

func BenchmarkPutGet(b *testing.B) {
	s := New()
	defer s.Close()
	val := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		s.Put(key, val)
		_, _ = s.Get(key)
	}
}
