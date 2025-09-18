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
	unsealed := s.FirstUnsealedSegment.FirstRecord()
	sealed := s.FirstSealedSegment.FirstRecord()
	if sealed.IsZero() {
		return unsealed
	} else if unsealed.IsZero() {
		return sealed
	} else if sealed.ID < unsealed.ID {
		return sealed
	} else {
		return unsealed
	}
}

func (s *Summary) UncommittedCount() int {
	if s.LastUncommitted.ID > s.LastCommitted.ID {
		return int(s.LastUncommitted.ID - s.LastCommitted.ID)
	}
	return 0
}
