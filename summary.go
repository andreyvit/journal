package journal

type Summary struct {
	FirstSealedSegment   Segment
	LastSealedSegment    Segment
	FirstUnsealedSegment Segment
	LastUnsealedSegment  Segment
	SegmentCount         int
	LastCommitted        Meta
	LastUncommitted      Meta
}

func (s *Summary) FirstRecord() Meta {
	if s.FirstUnsealedSegment.IsZero() {
		return Meta{}
	}
	return s.FirstUnsealedSegment.FirstRecord()
}

func (s *Summary) UncommittedCount() int {
	if s.LastUncommitted.ID > s.LastCommitted.ID {
		return int(s.LastUncommitted.ID - s.LastCommitted.ID)
	}
	return 0
}
