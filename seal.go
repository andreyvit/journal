package journal

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/andreyvit/sealer"
)

var ErrMissingSealKey = errors.New("missing seal key")

func (j *Journal) CanSeal() bool {
	return len(j.sealKeys) > 0
}

func (j *Journal) SealAndTrimOnce(ctx context.Context) (int, error) {
	var actions int
	if ctx.Err() == nil {
		seg, err := j.Seal(ctx)
		if err != nil {
			return actions, err
		}
		if !seg.IsZero() {
			actions++
		}
	}
	if ctx.Err() == nil {
		seg, err := j.Trim()
		if err != nil {
			return actions, err
		}
		if !seg.IsZero() {
			actions++
		}
	}
	return actions, nil
}

func (j *Journal) SealAndTrimAll(ctx context.Context) (int, error) {
	var actions int
	for ctx.Err() == nil {
		seg, err := j.Seal(ctx)
		if err != nil {
			return actions, err
		}
		if seg.IsZero() {
			break
		}
		actions++
	}
	for ctx.Err() == nil {
		seg, err := j.Trim()
		if err != nil {
			return actions, err
		}
		if seg.IsZero() {
			break
		}
		actions++
	}
	return actions, nil
}

func (j *Journal) Seal(ctx context.Context) (Segment, error) {
	if !j.CanSeal() {
		return Segment{}, nil
	}
	sealKey := j.sealKeys[0]

	next, err := j.nextToSeal()
	if err != nil {
		return Segment{}, err
	}
	if next.IsZero() || !next.status.CanSeal() {
		return Segment{}, nil
	}

	// Allow only one concurrent seal op. But don't wait for the lock;
	// we consider Seal operations opportunistic and expect them to be
	// reinvoked regularly.
	if !j.sealLock.TryLock() {
		return Segment{}, nil
	}
	defer j.sealLock.Unlock()

	// Recheck after obtaining seal lock to be sure.
	next, err = j.nextToSeal()
	if err != nil {
		return Segment{}, err
	}
	if next.IsZero() || !next.status.CanSeal() {
		return Segment{}, nil
	}

	if err := ctx.Err(); err != nil {
		return Segment{}, err
	}

	// Start sealing
	start := time.Now()

	inf, sr, err := openSegment(j, next)
	if err != nil {
		return Segment{}, err
	}
	defer inf.Close()

	inStat, err := inf.Stat()
	if err != nil {
		return Segment{}, err
	}
	inSize := inStat.Size()

	tempseg := next
	tempseg.status = sealingTemp
	j.setSealingTemp(tempseg)
	defer j.setSealingTemp(Segment{})

	finalseg := tempseg
	finalseg.status = Sealed
	final := j.filePath(finalseg.fileName(j))

	outf, err := j.openFile(tempseg, true)
	if err != nil {
		return tempseg, err
	}
	temp := outf.Name()

	var ok bool
	defer closeAndDeleteUnlessOK2(&outf, temp, &ok)

	var hbuf [segmentHeaderSize]byte
	fillSegmentHeader(hbuf[:], j, magicV1Sealed, tempseg.segnum, tempseg.ts, tempseg.recnum, sr.h.LastTimestamp, sr.h.LastRecordNumber)

	sealw, err := sealer.Seal(outf, sealKey, hbuf[:], j.sealOpts)
	if err != nil {
		return tempseg, err
	}

	ts := tempseg.ts
	var count int
	for {
		err := sr.next()
		if err == io.EOF {
			break
		} else if err != nil {
			return tempseg, err
		}

		var tsDelta uint64
		if sr.ts > ts {
			tsDelta = sr.ts - ts
			ts = sr.ts
		}

		err = writeSealedRecord(sealw, tsDelta, sr.data)
		if err != nil {
			return tempseg, err
		}

		count++
		if count%10 == 0 {
			if err := ctx.Err(); err != nil {
				return tempseg, err
			}
		}
	}

	err = sealw.Close()
	if err != nil {
		return tempseg, err
	}

	outSize, err := outf.Seek(0, io.SeekCurrent)
	if err != nil {
		return tempseg, err
	}

	err = outf.Close()
	outf = nil // prevent double close in closeAndDeleteUnlessOK2
	if err != nil {
		return tempseg, err
	}
	ok = true

	err = os.Rename(temp, final)
	if err != nil {
		return tempseg, err
	}

	elapsed := time.Since(start)
	j.logger.Debug("journal sealed", "journal", j.debugName, "dur", elapsed.Milliseconds(), "in_size", inSize, "out_size", outSize, "ns_per_kb", (elapsed / time.Duration((inSize+1023)/1024)).Nanoseconds())

	ok = true
	j.updateStateWithSegmentAdded(finalseg)
	return finalseg, nil
}

func (j *Journal) Trim() (Segment, error) {
	next, err := j.nextToTrim()
	if err != nil {
		return Segment{}, err
	}
	if next.IsZero() || !next.status.CanSeal() {
		return Segment{}, nil
	}

	// Allow only one concurrent trim op. But don't wait for the lock;
	// we consider Trim operations opportunistic and expect them to be
	// reinvoked regularly.
	if !j.sealLock.TryLock() {
		return Segment{}, nil
	}
	defer j.sealLock.Unlock()

	// Recheck after obtaining trim lock to be sure.
	next, err = j.nextToTrim()
	if err != nil {
		return Segment{}, err
	}
	if next.IsZero() || !next.status.CanSeal() {
		return Segment{}, nil
	}

	fn := j.filePath(next.fileName(j))
	err = os.Remove(fn)
	if err != nil {
		return next, err
	}

	j.updateStateWithSegmentGone(next)
	return next, nil
}

func writeSealedRecord(w io.Writer, tsDelta uint64, data []byte) error {
	var hbuf [maxRecHeaderLen]byte
	h := appendSealedRecordHeader(hbuf[:0], len(data), tsDelta)

	_, err := w.Write(h)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (j *Journal) findKey(keyID [sealer.IDSize]byte) *sealer.Key {
	for _, k := range j.sealKeys {
		if k.ID == keyID {
			return k
		}
	}
	return nil
}
