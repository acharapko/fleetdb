package wpaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/kv_store"
	"github.com/acharapko/fleetdb/ids"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(AcceptTX{})
	gob.Register(Accepted{})
	gob.Register(AcceptedTX{})
	gob.Register(Commit{})
	gob.Register(Exec{})
	gob.Register(CommitTX{})
	gob.Register(LeaderChange{})
}

/**************************
 * Inter-Replica Messages *
 **************************/

// Prepare phase 1a
type Prepare struct {
	Key    kv_store.Key
	Table  string
	Ballot Ballot
	txTime int64
	Try    int
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {Key=%v, Table=%s, bal=%v, Try=%d, txtime=%v}", string(p.Key), p.Table, p.Ballot, p.Try, p.txTime)
}

type CommandBallot struct {
	Command   kv_store.Command
	Ballot    Ballot
	Executed  bool
	Committed bool
	HasTx     bool
	Tx        fleetdb.Transaction
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("c=%v b=%v, commited=%t, exec=%t", cb.Command, cb.Ballot, cb.Committed, cb.Executed)
}

// Promise phase 1b
type Promise struct {
	Key    kv_store.Key
	Table  string
	Ballot Ballot
	ID     ids.ID               // from node id
	LPF    bool					//Lease Promise Failure
	Try    int
	Log    map[int]CommandBallot // log since last execute (includes last execute)
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {Key=%v, Table=%s, bal=%v, try=%d, id=%v, lpf=%t, log=%v}", string(p.Key), p.Table, p.Ballot, p.Try, p.ID, p.LPF, p.Log)
}

// Accept phase 2a
type Accept struct {
	Ballot    Ballot
	Slot      int
	EpochSlot int
	Command   kv_store.Command
	txtime    int64
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {Key=%v, bal=%v, Slot=%v, EpochSlot=%v, Cmd=%v, txtime=%d}", string(a.Command.Key), a.Ballot, a.Slot, a.EpochSlot, a.Command, a.txtime)
}

// Accept phase 2a for TX
type AcceptTX struct {
	TxID ids.TXID
	LeaderID ids.ID
	P2as []Accept
}

func (a AcceptTX) String() string {
	return fmt.Sprintf("AcceptTX {TxID=%s, TxOwner=%s, p2as = %v}", a.TxID, a.LeaderID, a.P2as)
}

// Accepted phase 2b
type Accepted struct {
	Table  string
	Key    kv_store.Key
	Ballot Ballot
	ID     ids.ID // from node id
	Slot   int
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {Table=%s, Key=%v, bal=%v, Slot=%v, ID=%v}", a.Table, string(a.Key), a.Ballot, a.Slot, a.ID)
}

// Accepted phase 2b
type AcceptedTX struct {
	TxID ids.TXID
	P2bs []Accepted
}

func (a AcceptedTX) String() string {
	return fmt.Sprintf("Accepted {TxID=%s, p2bs = %v}", a.TxID, a.P2bs)
}

// Commit phase 3
type Commit struct {
	Slot    int
	Command kv_store.Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {Key=%v, Slot=%v, cmd=%v}", string(c.Command.Key), c.Slot, c.Command)
}

// Commit phase 3 - outside Q2
type Exec struct {
	Slot      int
	EpochSlot int
	Command   kv_store.Command
}

func (e Exec) String() string {
	return fmt.Sprintf("Exec {Key=%v, Slot=%v, EpochSlot=%v, cmd=%v}", string(e.Command.Key), e.Slot, e.EpochSlot, e.Command)
}

// Commit phase 3
type CommitTX struct {
	ids.TXID
	P3s []Commit

}

func (c CommitTX) String() string {
	return fmt.Sprintf("CommitTX {txid=%v, %v}", c.TXID, c.P3s)
}


// LeaderChange switch leader
type LeaderChange struct {
	Key    kv_store.Key
	Table  string
	To     ids.ID
	From   ids.ID
	Ballot Ballot
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {Key=%s, Table=%s, from=%s, to=%s, bal=%d}", string(l.Key), l.Table, l.From, l.To, l.Ballot)
}

// load Gossip

type GossipBalance struct {
	Items int64
}

func (gb GossipBalance) String() string {
	return fmt.Sprintf("GossipBalance {balance=%d}", gb.Items)
}
