package workload

import (
	"strconv"
	"strings"
	"time"
)

// formatValue encodes a counter value + write timestamp into "<n>:<unix-nano>".
func formatValue(n int64, t time.Time) string {
	return strconv.FormatInt(n, 10) + ":" + strconv.FormatInt(t.UnixNano(), 10)
}

// parseValue is the inverse of formatValue. ok is false if the string can't be
// parsed (e.g., not yet written, or a non-russ key collided).
func parseValue(s string) (n int64, writtenAt time.Time, ok bool) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, time.Time{}, false
	}
	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, time.Time{}, false
	}
	ts, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, time.Time{}, false
	}
	return n, time.Unix(0, ts), true
}
