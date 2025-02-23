package journal

import "time"

func ToTimestamp(t time.Time) uint64 {
	return uint64(t.UnixMilli())
}

func ToTime(ts uint64) time.Time {
	return time.UnixMilli(int64(ts))
}
