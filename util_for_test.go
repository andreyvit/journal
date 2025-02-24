package journal_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func concat(items ...string) string {
	return strings.Join(items, " ")
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
