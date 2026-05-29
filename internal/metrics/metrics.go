package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "vaultfs"

// RequestsTotal counts HTTP requests by method and response status code.
var RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "requests_total",
	Help:      "Total HTTP requests processed.",
}, []string{"method", "status"})

// RequestDurationSeconds measures HTTP request latency in seconds.
var RequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "request_duration_seconds",
	Help:      "HTTP request latency in seconds.",
	Buckets:   prometheus.DefBuckets,
}, []string{"method"})

// ReplicationLagSeconds is the time since the last successful replication to a peer.
var ReplicationLagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "replication_lag_seconds",
	Help:      "Seconds since the last successful replication to a peer.",
}, []string{"peer"})

// KeysTotal is the number of keys currently stored in memory.
var KeysTotal = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "keys_total",
	Help:      "Current number of keys in the store.",
})

// ObserveRequest records request count and duration.
func ObserveRequest(method string, status int, duration time.Duration) {
	RequestsTotal.WithLabelValues(method, strconv.Itoa(status)).Inc()
	RequestDurationSeconds.WithLabelValues(method).Observe(duration.Seconds())
}

// SetKeysTotal updates the key count gauge.
func SetKeysTotal(count int) {
	KeysTotal.Set(float64(count))
}

// SetReplicationLag sets replication lag for a peer in seconds.
func SetReplicationLag(peer string, lagSeconds float64) {
	ReplicationLagSeconds.WithLabelValues(peer).Set(lagSeconds)
}

// DeleteReplicationLag removes the lag gauge for a peer no longer configured.
func DeleteReplicationLag(peer string) {
	ReplicationLagSeconds.DeleteLabelValues(peer)
}
