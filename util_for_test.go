package journal_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andreyvit/journal"
)

func testLogger(t testing.TB) *slog.Logger {
	return slog.New(slog.NewTextHandler(&logWriter{t}, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	}))
}

type logWriter struct{ t testing.TB }

func (c *logWriter) Write(buf []byte) (int, error) {
	msg := string(buf)
	origLen := len(msg)
	msg = strings.TrimSuffix(msg, "\n")
	c.t.Log(msg)
	return origLen, nil
}

type fakeClock atomic.Uint64

func newClock() *fakeClock {
	c := &fakeClock{}
	c.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	return c
}

func (c *fakeClock) Now() time.Time {
	a := (*atomic.Uint64)(c)
	return journal.ToTime(a.Load())
}

func (c *fakeClock) NowTS() uint64 {
	a := (*atomic.Uint64)(c)
	return a.Load()
}

func (c *fakeClock) Set(now time.Time) {
	a := (*atomic.Uint64)(c)
	a.Store(journal.ToTimestamp(now))
}

func (c *fakeClock) Advance(d time.Duration) {
	a := (*atomic.Uint64)(c)
	a.Add(uint64(d.Milliseconds()))
}

func at(s string) uint64 {
	return must(journal.ParseTime(s))
}

func concat(items ...string) string {
	return strings.Join(items, " ")
}

func ok(t testing.TB, ok bool) {
	if !ok {
		t.Helper()
		t.Fatalf("** condition mismatched")
	}
}

func eq[T comparable](t testing.TB, a, e T) {
	if a != e {
		t.Helper()
		t.Fatalf("** got %v, wanted %v", a, e)
	}
}

func eqstr(t testing.TB, a, e []byte) {
	if !bytes.Equal(a, e) {
		t.Helper()
		t.Fatalf("** got %q, wanted %q", a, e)
	}
}

func deepEq[T any](t testing.TB, a, e T) bool {
	if !reflect.DeepEqual(a, e) {
		t.Helper()
		t.Fatalf("** got %v, wanted %v", a, e)
		return false
	}
	return true
}

func bytesEq(t testing.TB, a, e []byte) bool {
	if !bytes.Equal(a, e) {
		an, en := len(a), len(e)
		off := min(an, en)
		for i := range min(an, en) {
			if a[i] != e[i] {
				off = i
				break
			}
		}

		t.Helper()
		t.Fatalf("** got:\n%v\nwanted:\n%v\nfirst difference offset: 0x%x (%d)", hexDump(a, off), hexDump(e, off), off, off)
		return false
	}
	return true
}

func success(t testing.TB, a error) bool {
	if a != nil {
		t.Helper()
		t.Errorf("** failed: %v", a)
		return false
	}
	return true
}

func hexDump(b []byte, highlightOff int) string {
	var buf strings.Builder
	var off int
	n := len(b)
	for {
		fmt.Fprintf(&buf, "%08x", off)
		if off >= n {
			buf.WriteByte('\n')
			break
		}
		buf.WriteByte(' ')
		for i := range 8 {
			if off+i >= n {
				buf.WriteByte(' ')
				buf.WriteByte(' ')
				buf.WriteByte(' ')
			} else {
				if highlightOff >= 0 && off+i == highlightOff {
					buf.WriteByte('>')
				} else {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%02x", b[off+i])
			}
		}
		buf.WriteByte(' ')
		buf.WriteByte(' ')
		buf.WriteByte('|')
		for i := range 8 {
			if off+i < n {
				v := b[off+i]
				if v >= 32 && v <= 126 {
					buf.WriteByte(v)
				} else {
					buf.WriteByte('.')
				}
			}
		}
		off += 8
		buf.WriteByte('|')
		buf.WriteByte('\n')
		if off >= n {
			break
		}
	}
	return buf.String()
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ensure(err error) {
	if err != nil {
		panic(err)
	}
}

func expand(specs ...string) []byte {
	var b []byte
	for _, spec := range specs {
		for _, elem := range strings.Fields(spec) {
			base, _, _ := strings.Cut(elem, "/") // comment
			if base == "" {
				continue
			}

			base, repStr, _ := strings.Cut(base, "*")

			rep := 1
			if repStr != "" {
				var err error
				rep, err = strconv.Atoi(repStr)
				if err != nil {
					panic(fmt.Sprintf("invalid repeat count %q in element %q", repStr, elem))
				}
			}

			base, right, padTo8 := strings.Cut(base, "...")
			var padTo4 bool
			if !padTo8 {
				base, right, padTo4 = strings.Cut(base, "..")
			}

			baseBytes, err := appendHexDecoding(nil, base)
			if err != nil {
				panic(fmt.Errorf("%w in element %q", err, elem))
			}

			rightBytes, err := appendHexDecoding(nil, right)
			if err != nil {
				panic(fmt.Errorf("%w in element %q", err, elem))
			}

			for range rep {
				b = append(b, baseBytes...)

				n := len(baseBytes) + len(rightBytes)
				if padTo8 && n < 8 {
					for range 8 - n {
						b = append(b, 0)
					}
				} else if padTo4 && n < 4 {
					for range 4 - n {
						b = append(b, 0)
					}
				}

				b = append(b, rightBytes...)
			}
		}
	}
	return b
}

func appendHexDecoding(data []byte, hex string) ([]byte, error) {
	const none byte = 0xFF

	if decimal, ok := strings.CutPrefix(hex, "#"); ok {
		v, err := strconv.ParseUint(decimal, 10, 64)
		if err != nil {
			return nil, err
		}
		return binary.AppendUvarint(data, v), nil
	} else if alpha, ok := strings.CutPrefix(hex, "'"); ok {
		return append(data, alpha...), nil
	}

	prev := none
	for _, b := range []byte(hex) {
		var half byte
		switch b {
		case '_', ' ':
			if prev != none {
				data = append(data, prev)
				prev = none
			}
			continue
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			half = b - '0'
		case 'a', 'b', 'c', 'd', 'e', 'f':
			half = b - 'a' + 10
		case 'A', 'B', 'C', 'D', 'E', 'F':
			half = b - 'A' + 10
		default:
			return nil, fmt.Errorf("invalid char '%c'", b)
		}
		if prev == none {
			prev = half
		} else {
			data = append(data, prev<<4|half)
			prev = none
		}
	}
	if prev != none {
		data = append(data, prev)
	}
	return data, nil
}

func recsStr(recs []journal.Record, first uint64) []string {
	var b strings.Builder
	var result []string
	for i, rec := range recs {
		b.Reset()
		if e := first + uint64(i); rec.ID != e {
			fmt.Fprintf(&b, "[**ID=%d,wanted=%d**]", rec.ID, e)
		}
		b.WriteString(journal.TimeToStr(rec.Time()))
		b.WriteByte(':')
		b.Write(rec.Data)
		result = append(result, b.String())
	}
	return result
}

func recsEq(t testing.TB, recs []journal.Record, start uint64, e ...string) bool {
	a := recsStr(recs, start)
	if !reflect.DeepEqual(a, e) {
		t.Helper()
		t.Fatalf("** got:\n%v\n\nwanted:\n%v", a, e)
		return false
	}
	return true
}
