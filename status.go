package journal

import "strings"

type Status uint8

const (
	Invalid Status = iota
	Draft
	Finalized
	Sealed
	sealingTemp
)

const (
	finalizedPrefix   = "F"
	draftPrefix       = "W"
	sealedPrefix      = "S"
	sealingTempPrefix = "T"
)

func (s Status) IsSealed() bool { return s == Sealed }

func (s Status) IsDraft() bool { return s == Draft }

func (s Status) CanSeal() bool { return s == Finalized }

func cutStatusPrefix(s string) (Status, string) {
	if r, ok := strings.CutPrefix(s, finalizedPrefix); ok {
		return Finalized, r
	} else if r, ok := strings.CutPrefix(s, draftPrefix); ok {
		return Draft, r
	} else if r, ok := strings.CutPrefix(s, sealedPrefix); ok {
		return Sealed, r
	} else if r, ok := strings.CutPrefix(s, sealingTempPrefix); ok {
		return sealingTemp, r
	}
	return Invalid, s
}

func (s Status) prefix() string {
	switch s {
	case Draft:
		return draftPrefix
	case Finalized:
		return finalizedPrefix
	case Sealed:
		return sealedPrefix
	case sealingTemp:
		return sealingTempPrefix
	default:
		return ""
	}
}
