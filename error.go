package journal

import "fmt"

type fsyncFailedError struct {
	Cause error
}

func (e *fsyncFailedError) Error() string {
	return fmt.Sprintf("fsync failed (unrecoverable without server reboot): %v", e.Cause)
}

func (e *fsyncFailedError) Unwrap() error {
	return e.Cause
}
