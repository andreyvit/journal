package journal

import "os"

func (j *Journal) syncDirectory() error {
	file, err := os.Open(j.dir)
	if err != nil {
		return &fsyncFailedError{Cause: err}
	}
	defer file.Close()
	if err = j.syncFile(file); err != nil {
		return &fsyncFailedError{Cause: err}
	}
	return nil
}

func (sw *segmentWriter) sync() error {
	if sw.modified {
		if err := sw.j.syncFile(sw.f); err != nil {
			return &fsyncFailedError{Cause: err}
		}
	}
	if !sw.dirSynced {
		if err := sw.j.syncDirectory(); err != nil {
			return err
		}
		sw.dirSynced = true
	}
	sw.modified = false
	return nil
}
