package wpaxos2

import (
	"encoding/gob"
	"fmt"

	"github.com/acharapko/fleetdb"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(AcceptTX{})
	gob.Register(Accepted{})
	gob.Register(AcceptedTX{})
	gob.Register(Commit{})
	gob.Register(CommitTX{})
	gob.Register(LeaderChange{})
}

/**************************
 * Inter-Replica Messages *
 **************************/

// Prepare phase 1a
type Prepare struct {
	Key fleetdb.Key
	Ballot fleetdb.Ballot
	txTime int64
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {Key=%v, bal=%v, txtime=%v}", string(p.Key), p.Ballot, p.txTime)
}

type CommandBallot struct {
	Command fleetdb.Command
	Ballot  fleetdb.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("c=%v b=%v", cb.Command, cb.Ballot)
}

// Promise phase 1b
type Promise struct {
	Key fleetdb.Key
	Ballot fleetdb.Ballot
	ID     fleetdb.ID               // from node id
	LPF	   bool					//Lease Promise Failure
	Log    map[int]CommandBallot // uncommitted logs
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {Key=%v, bal=%v, id=%v, log=%v}", string(p.Key), p.Ballot, p.ID, p.Log)
}

// Accept phase 2a
type Accept struct {
	Key fleetdb.Key
	Ballot  fleetdb.Ballot
	Slot    int
	Command fleetdb.Command
	txtime int64
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {Key=%v, bal=%v, Slot=%v, Cmd=%v, txtime=%d}", string(a.Key), a.Ballot, a.Slot, a.Command, a.txtime)
}

// Accept phase 2a for TX
type AcceptTX struct {
	TxID fleetdb.TXID
	LeaderID fleetdb.ID
	P2as []Accept
}

func (a AcceptTX) String() string {
	return fmt.Sprintf("AcceptTX {TxID=%s, TxOwner=%s, p2as = %v}", a.TxID, a.LeaderID, a.P2as)
}

// Accepted phase 2b
type Accepted struct {
	Key fleetdb.Key
	Ballot fleetdb.Ballot
	ID     fleetdb.ID // from node id
	Slot   int
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {Key=%v, bal=%v, Slot=%v, ID=%v}", string(a.Key), a.Ballot, a.Slot, a.ID)
}

// Accepted phase 2b
type AcceptedTX struct {
	TxID fleetdb.TXID
	P2bs []Accepted
}

func (a AcceptedTX) String() string {
	return fmt.Sprintf("Accepted {TxID=%s, p2bs = %v}", a.TxID, a.P2bs)
}

// Commit phase 3
type Commit struct {
	Key fleetdb.Key
	Slot    int
	Command fleetdb.Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {Key=%v, Slot=%v, cmd=%v}", string(c.Key), c.Slot, c.Command)
}

// Commit phase 3
type CommitTX struct {
	fleetdb.TXID
	P3s []Commit

}

func (c CommitTX) String() string {
	return fmt.Sprintf("CommitTX {txid=%v, %v}", c.TXID, c.P3s)
}


// LeaderChange switch leader
type LeaderChange struct {
	Key    fleetdb.Key
	To     fleetdb.ID
	From   fleetdb.ID
	Ballot fleetdb.Ballot
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {Key=%d, from=%s, to=%s, bal=%d}", string(l.Key), l.From, l.To, l.Ballot)
}

// Load Gossip

type GossipBalance struct {
	Items int64
}

func (gb GossipBalance) String() string {
	return fmt.Sprintf("GossipBalance {balance=%d}", gb.Items)
}
