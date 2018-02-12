package db

import (
	"time"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
)

type hitstat interface {
	Hit(id fleetdb.ID, lt int64) fleetdb.ID
	HitWeight(id fleetdb.ID, weight int, lt int64) fleetdb.ID
	Reset()
	CalcDestination() fleetdb.ID
	LastReqTime() int64
	Evicting() bool
	MarkEvecting()
}

// stat of access history in previous interval time
type stat struct {
	hits     map[fleetdb.ID]int
	time     time.Time // last start time
	LastReqT int64 //last op timestamp
	evicting bool
	sum      int       // total hits in current interval
}

func newStat() *stat {
	return &stat{
		hits:     make(map[fleetdb.ID]int),
		time:     time.Now(),
	}
}

// hit record access id and return the
func (s *stat) CalcDestination() fleetdb.ID {
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
/*func (s *stat) TotalHitsPerSec() int {
	ts := time.Since(s.time)
	thps := float64(s.sum) / ts.Seconds()
	//log.Debugf("s.sum = %d, ts.sec =%f\n", s.sum, ts.Seconds())
	return int(thps)
}*/

func (s *stat) LastReqTime() int64 {
	return s.LastReqT
}

func (s *stat) Evicting() bool {
	return s.evicting
}

func (s *stat) MarkEvecting() {
	s.evicting = true
}

// hit record access id and return the
func (s *stat) Hit(id fleetdb.ID, lt int64) fleetdb.ID {
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
func (s *stat) HitWeight(id fleetdb.ID, weight int, lt int64) fleetdb.ID {
	s.hits[id] += weight
	s.sum += weight
	s.LastReqT = lt
	if time.Since(s.time) >= time.Millisecond*time.Duration(fleetdb.HandoverInterval) {
		return s.CalcDestination()
	}
	return ""
}

func (s *stat) Reset() {
	s.hits = make(map[fleetdb.ID]int)
	s.sum = 0
	s.evicting = false
	s.time = time.Now()
}
