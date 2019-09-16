package wpaxos

import (
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/ids"
)

var (
	// NumZones total number of sites
	NumZones int
	// NumNodes total number of nodes
	NumNodes int
	// NumLocalNodes number of nodes per site
	NumLocalNodes int
	// F number of zone failures
	F int
	// QuorumType name of the quorums
	QuorumType string
)

type Quorum struct {
	size  int
	acks  map[ids.ID]bool
	zones map[uint8]int8
	nacks map[ids.ID]bool
}

func NewQuorum() *Quorum {
	return &Quorum{
		size:  0,
		acks:  make(map[ids.ID]bool, NumNodes),
		zones: make(map[uint8]int8, NumZones),
		nacks: make(map[ids.ID]bool, NumNodes),
	}
}

func (q *Quorum) ACK(id ids.ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

func (q *Quorum) NACK(id ids.ID) {
	if !q.nacks[id] {
		q.nacks[id] = true
	}
}

func (q *Quorum) ADD() {
	q.size++
}

func (q *Quorum) Size() int {
	return q.size
}

func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ids.ID]bool, NumNodes)
	q.zones = make(map[uint8]int8, NumZones)
	q.nacks = make(map[ids.ID]bool, NumNodes)
}

func (q *Quorum) Majority() bool {
	return q.size > NumNodes/2
}

func (q *Quorum) FastQuorum() bool {
	return q.size >= NumNodes-1
}

func (q *Quorum) FastPath() bool {
	return q.size >= NumNodes*3/4
}

func (q *Quorum) AllZones() bool {
	return len(q.zones) == NumZones
}

func (q *Quorum) ZoneMajority() bool {
	for _, n := range q.zones {
		if int(n) > NumLocalNodes/2 {
			return true
		}
	}
	return false
}

func (q *Quorum) GridRow() bool {
	return q.AllZones()
}

func (q *Quorum) GridColumn() bool {
	for _, n := range q.zones {
		if int(n) == NumLocalNodes {
			return true
		}
	}
	return false
}

func (q *Quorum) FGridQ1() bool {
	z := 0
	for _, n := range q.zones {
		if int(n) > NumLocalNodes/2 {
			z++
		}
	}
	return z >= NumZones-F
}

func (q *Quorum) FGridQ2() bool {
	z := 0
	for _, n := range q.zones {
		if int(n) > NumLocalNodes/2 {
			z++
		}
	}
	return z >= F+1
}

func (q *Quorum) Q1() bool {
	switch QuorumType {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridRow()
	case "fgrid":
		return q.FGridQ1()
	case "group":
		return q.ZoneMajority()
	default:
		log.Errorln("Unknown quorum type")
		return false
	}
}

func (q *Quorum) Q2() bool {
	switch QuorumType {
	case "majority":
		return q.Majority()
	case "grid":
		return q.GridColumn()
	case "fgrid":
		return q.FGridQ2()
	case "group":
		return q.ZoneMajority()
	default:
		log.Errorln("Unknown quorum type")
		return false
	}
}
