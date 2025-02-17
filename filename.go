package journal

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var timestampRe = regexp.MustCompile(`^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(\d{3})$`)

func formatSegmentName(prefix, suffix string, seg uint32, ts uint64, id uint64) string {
	t := time.UnixMilli(int64(ts)).UTC()
	return fmt.Sprintf("%s%010d-%04d%02d%02dT%02d%02d%02d%03d-%012d%s", prefix, seg, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond()/1e6, id, suffix)
}

func parseSegmentName(prefix, suffix, name string) (seg uint32, ts uint64, id uint64, err error) {
	origName := name
	name, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	name, ok = strings.CutSuffix(name, suffix)
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}

	segStr, rem, ok := strings.Cut(name, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	v, err := strconv.ParseUint(segStr, 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q (invalid segment number)", origName)
	}
	seg = uint32(v)

	tsStr, idStr, ok := strings.Cut(rem, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	var t time.Time
	if m := timestampRe.FindStringSubmatch(tsStr); m != nil {
		t = time.Date(atoi(m[1]), time.Month(atoi(m[2])), atoi(m[3]), atoi(m[4]), atoi(m[5]), atoi(m[6]), atoi(m[7])*1000_000, time.UTC)
	} else {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q (invalid timestamp)", origName)
	}
	ts = uint64(t.UnixMilli())

	id, err = strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return seg, 0, 0, fmt.Errorf("invalid segment file name %q (invalid record identifier)", origName)
	}
	return
}

func atoi(s string) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return v
}
