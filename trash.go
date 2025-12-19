package journal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func (j *Journal) quarantineFile(path string) error {
	if path == "" {
		return fmt.Errorf("journal: empty path")
	}
	trashDir := j.trashPath
	if trashDir == "" {
		trashDir = filepath.Join(j.dir, "trash")
	}
	if err := os.MkdirAll(trashDir, 0o755); err != nil {
		return err
	}
	dst, err := uniqueTrashPath(filepath.Join(trashDir, filepath.Base(path)))
	if err != nil {
		return err
	}
	if path == dst {
		return nil
	}
	if err := os.Rename(path, dst); err != nil {
		var linkErr *os.LinkError
		if errors.As(err, &linkErr) && errors.Is(linkErr.Err, syscall.EXDEV) {
			if err := copyFile(path, dst); err != nil {
				return err
			}
			return os.Remove(path)
		}
		return err
	}
	return nil
}

func (j *Journal) quarantineSegment(seg Segment, cause error) error {
	path := j.filePath(seg.fileName(j))
	if err := j.quarantineFile(path); err != nil {
		if os.IsNotExist(err) {
			j.updateStateWithSegmentGone(seg)
			j.logger.Warn("journal corrupted segment missing", "journal", j.debugName, "segment", seg.String())
			return nil
		}
		return err
	}
	j.updateStateWithSegmentGone(seg)
	if cause != nil {
		j.logger.Warn("journal moved corrupted segment to trash", "journal", j.debugName, "segment", seg.String(), "err", cause)
	} else {
		j.logger.Warn("journal moved corrupted segment to trash", "journal", j.debugName, "segment", seg.String())
	}
	return nil
}

func uniqueTrashPath(path string) (string, error) {
	if _, err := os.Stat(path); err == nil {
		return nextTrashPath(path)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return path, nil
}

func nextTrashPath(path string) (string, error) {
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 2; ; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s-%d%s", base, i, ext))
		if _, err := os.Stat(candidate); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return "", err
		}
		return candidate, nil
	}
}

func copyFile(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, info.Mode().Perm())
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}
