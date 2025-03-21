package journal

import (
	"math"
	"os"
)

func allocSize(sz int) int {
	if sz >= math.MaxInt64/2 {
		panic("size too large")
	}
	r := 64 * 1024
	for r < sz {
		r <<= 1
	}
	return r
}

func closeAndDeleteUnlessOK(f *os.File, ok *bool) {
	if *ok {
		return
	}
	f.Close()
	os.Remove(f.Name())
}

func closeAndDeleteUnlessOK2(f **os.File, name string, ok *bool) {
	if *ok {
		return
	}
	if ff := *f; ff != nil {
		ff.Close()
	}
	os.Remove(name)
}

func closeUnlessOK(f *os.File, ok *bool) {
	if *ok {
		return
	}
	f.Close()
}

func closeIfNonNil(pf **os.File) {
	if f := *pf; f != nil {
		f.Close()
	}
}
