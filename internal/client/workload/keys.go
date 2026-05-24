package workload

// Shared key names used by the writer, master reader, and replica observer.
// The writer maintains a monotonic counter at counterKey and a "<n>:<unix-nano-ts>"
// value at latestKey; both readers parse latestKey to extract the counter
// value and the wall-clock time the master originally wrote it.
const (
	counterKey = "russ:client:counter"
	latestKey  = "russ:client:latest"
)
