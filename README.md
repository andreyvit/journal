# journal

Append-only, segmented journals for WALs and logs, with commit boundaries, timestamps, rotation and optional compression/encryption.

## Install

Requires Go 1.23.3 or newer.

```sh
go get github.com/andreyvit/journal@latest
```

## Example

```go
package main

import (
	"fmt"
	"os"

	"github.com/andreyvit/journal"
)

func main() {
	dir, err := os.MkdirTemp("", "journal-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	j := journal.New(dir, journal.Options{FileName: "events-*.wal"})
	j.StartWriting()
	if err := j.WriteRecord(j.Now(), []byte("hello")); err != nil {
		panic(err)
	}
	if err := j.Commit(); err != nil {
		panic(err)
	}
	if err := j.FinishWriting(); err != nil {
		panic(err)
	}
	for rec := range j.Records(journal.Filter{}, func(err error) { panic(err) }) {
		fmt.Println(string(rec.Data)) // hello
	}
}
```

Check errors from writes, commits and `FinishWriting`. Records returned by iteration may borrow buffers; copy `rec.Data` if retaining it. The example leaves sealing disabled; configure `SealKeys` for encrypted segments. See [Options](https://pkg.go.dev/github.com/andreyvit/journal#Options) for rotation and commit settings.

## License

See [LICENSE](LICENSE).
