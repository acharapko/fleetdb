package wpaxos

import (
	"time"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/ids"
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
	time     time.Time // last start time
	LastReqT int64 //last op timestamp
	evicting bool
	sum      int       // total hits in current interval
}

func NewStat() *keystat {
	return &keystat{
		hits:     make(map[ids.ID]int),
		time:     time.Now(),
	}
}

// hit record access id and return the
func (s *keystat) CalcDestination() ids.ID {

	for id, n := range s.hits {
		if n > s.sum / 2 {
			// TODO should we reset for every interval?
			log.Debugf("Reset \n")
			s.Reset()
			return id
		}
	}
	log.Debugf("No Reset \n")
	return ""
}


// total hits per second truncated to an int
/*func (s *keystat) TotalHitsPerSec() int {
	ts := time.Since(s.time)
	thps := float64(s.sum) / ts.Seconds()
	//log.Debugf("s.sum = %d, ts.sec =%f\n", s.sum, ts.Seconds())
	return int(thps)
}*/

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
