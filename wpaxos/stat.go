package wpaxos

import (
	"time"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/ids"
	"math"

)

type hitstat interface {
	Hit(id ids.ID, lt int64) ids.ID
	HitWeight(id ids.ID, weight int, lt int64) ids.ID
	Reset()
	CalcDestination() ids.ID
	LastReqTime() int64
	Evicting() bool
	MarkEvicting()
}

// keystat of access history in previous interval time
type keystat struct {
	hits     map[ids.ID]int
	ema		 float64
	time     time.Time // last start time
	LastReqT int64 //last operation timestamp
	evicting bool
	sum      int // total hits in current interval
}

func NewStat() *keystat {
	return &keystat{
		hits:     make(map[ids.ID]int),
		ema:	  0,
		time:     time.Now(),
	}
}

func (s *keystat) CalcDestination() ids.ID {

	threshold := int(math.Ceil(1.0 / float64(NumZones) + Migration_majority) * float64(s.sum))

	for id, n := range s.hits {
		if n >= threshold {
			return id
		}
	}
	s.Reset()
	return ""
}

func (s *keystat) LastReqTime() int64 {
	return s.LastReqT
}

func (s *keystat) Evicting() bool {
	return s.evicting
}

func (s *keystat) MarkEvicting() {
	s.evicting = true
}

// hit record access id and return the
func (s *keystat) Hit(id ids.ID, lt int64) ids.ID {

	s.hits[id]++
	s.sum++
	s.LastReqT = lt
	//log.Debugf("Hit %f s int= %d ms, %d\n",time.Since(s.time).Seconds(), fleetdb.HandoverInterval, s.sum)

	//if time.Since(s.time) >= time.Millisecond*time.Duration(s.interval) {
	//	return s.CalcDestination()
	//}

	if (s.sum >= fleetdb.HandoverN && time.Since(s.time) >= time.Millisecond*time.Duration(fleetdb.HandoverInterval)) {
		return s.CalcDestination()
	}

	return ""
}

// hit record access id and return the
func (s *keystat) HitWeight(id ids.ID, weight int, lt int64) ids.ID {
	s.hits[id] += weight
	s.sum += weight
	s.LastReqT = lt
	if time.Since(s.time) >= time.Millisecond*time.Duration(fleetdb.HandoverInterval) {
		return s.CalcDestination()
	}
	return ""
}

func (s *keystat) Reset() {
	s.hits = make(map[ids.ID]int)
	s.sum = 0
	s.evicting = false
	s.time = time.Now()
}
