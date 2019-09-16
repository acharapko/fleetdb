package wpaxos

import (
	"github.com/acharapko/fleetdb/ids"
	"math"
	"github.com/acharapko/fleetdb/config"
)

type hitstat interface {
	Hit(id ids.ID, timestamp int64) ids.ID
	Reset()
	GetDestination() ids.ID
	LastReqTime() int64
	Evicting() bool
	MarkEvicting()
}

// keystat of access history in previous interval time
type keystat struct {
	regionScores    map[uint8]float32
	LastReqTimesamp int64     //last operation timestamp
	evicting        bool
}

func NewStat() *keystat {
	zones := config.Instance.GetZoneIds()
	scores := make(map[uint8]float32)
	for _, z := range zones {
		scores[z] = 1.0 / float32(len(zones))
	}
	return &keystat{
		regionScores: scores,
	}

}

func (s *keystat) GetDestination() ids.ID {
	minScore := float32(1.0)
	minRegion := uint8(math.MaxInt8)

	for z, _ := range s.regionScores {
		if s.regionScores[z] < minScore {
			minScore = s.regionScores[z]
			minRegion = z
		}
	}

	return ids.NewID(minRegion, 1)
}

func (s *keystat) LastReqTime() int64 {
	return s.LastReqTimesamp
}

func (s *keystat) Evicting() bool {
	return s.evicting
}

func (s *keystat) MarkEvicting() {
	s.evicting = true
}


// hit record access id and return the
func (s *keystat) Hit(id ids.ID, timestamp int64) ids.ID {
	s.LastReqTimesamp = timestamp
	alpha := config.Instance.CoGAlpha

	change := float32(0.0)
	minScore := float32(1.0)
	minRegion := uint8(math.MaxInt8)

	for z, score := range s.regionScores {
		if z != id.Zone() {
			change += alpha * score
			s.regionScores[z] -= alpha * score
			if s.regionScores[z] < minScore {
				minScore = s.regionScores[z]
				minRegion = z
			}
		}
	}
	s.regionScores[id.Zone()] += change
	if s.regionScores[id.Zone()] < minScore {
		minScore = s.regionScores[id.Zone()]
		minRegion = id.Zone()
	}

	return ids.NewID(minRegion, 1)
}



func (s *keystat) Reset() {
	s.regionScores = make(map[uint8]float32)
	s.evicting = false
}
