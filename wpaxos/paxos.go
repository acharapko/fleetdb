package wpaxos

import (
	"time"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"fmt"
	"encoding/binary"
	//"runtime/debug"
	"errors"
	"sort"
	"github.com/acharapko/fleetdb/key_value"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/utils"
)

var (
	ErrTXPreempted = errors.New("TX was preempted before issuing a lease")
)

// entry in log
type entry struct {
	ballot    Ballot
	command   key_value.Command
	commit    bool
	executed  bool

	tx        *fleetdb.Transaction
	request   *fleetdb.Request

	quorum    *Quorum
	timestamp int64

	oldLeader *ids.ID
}

func (e entry) String() string {
	return fmt.Sprintf("Slot Entry {bal=%s, cmd=%v, commit=%v, exec=%v, tx=%v}", e.ballot, e.command, e.commit, e.executed, e.tx)
}

// Paxos instance
type Paxos struct {
	fleetdb.Node

	Table			*string
	Key 			key_value.Key

	log     		map[int]*entry  // log ordered by slot
	execute 		int             // next execute slot number
	lastMutate 		int             // last slot number to mutate the value
	Active  		bool            // Active leader
	Try		 		int			//Try Number for P1a
	ballot  		Ballot // highest ballot number
	slot    		int             // highest slot number
	epochSlot		int				// starting execute slot for this epoch

	txLeaseDuration time.Duration
	txLeaseStart    time.Time
	txTime			int64
	txLease			ids.TXID

	quorum   		*Quorum    // phase 1 quorum
	requests 		[]*fleetdb.Request // phase 1 pending requests

	//lockNum	int

	//sync.RWMutex //Lock to control concurrent access to the same paxos instance

	Token 			chan bool

}

// NewPaxos creates new paxos instance
func NewPaxos(n fleetdb.Node, key key_value.Key, table *string) *Paxos {
	plog := make(map[int]*entry, n.Config().BufferSize)
	//log[0] = &entry{}
	p := &Paxos{
		Node:            n,
		log:             plog,
		execute:         1,
		lastMutate:		 0,
		Key:             key,
		Table:			 table,
		txLeaseDuration: time.Duration(n.Config().TX_lease) * time.Millisecond,
		quorum:          NewQuorum(),
		requests:        make([]*fleetdb.Request, 0),
		Token:			 make(chan bool, 1),
		epochSlot:		 1,
	}
	log.Debugf("Init Paxos for key %v\n", string(key))
	p.ReleaseAccessToken()
	return p
}

// IsLeader indicates if this node is current leader
func (p *Paxos) IsLeader() bool {
	return p.Active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader() ids.ID {
	return p.ballot.ID()
}

// ballot returns current ballot
func (p *Paxos) Ballot() Ballot {
	return p.ballot
}

// SlotNum returns current ballot
func (p *Paxos) SlotNum() int {
	return p.slot
}

func (p *Paxos) GetAccessToken() bool {
	//log.Debugf("Acquiring Token %v\n", string(p.Key))
	t := <- p.Token
	return t
}

func (p *Paxos) ReleaseAccessToken() {
	//debug.PrintStack()
	//p.lockNum++
	p.Token <- true //p.lockNum
	//log.Debugf("Released Token %v-%d\n", string(p.Key), p.lockNum)
}


// HandleRequest handles request and start phase 1 or phase 2
func (p *Paxos) HandleRequest(r fleetdb.Request) {
	//p.Lock()
	p.GetAccessToken()
	defer p.ReleaseAccessToken()
	log.Debugf("Replica %s received %v\n", p.ID(), r)
	if !p.Active {
		p.AddRequest(r)
		// current phase 1 pending
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		p.P2a(&r)
	}

}

func (p *Paxos) AddRequest(req fleetdb.Request) {
	p.requests = append(p.requests, &req)
}

// P1a starts phase 1 prepare
func (p *Paxos) P1a() {

	if p.Active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	m := Prepare{Key: p.Key, Table: *p.Table, Ballot: p.ballot, Try:p.Try}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

// P1a starts phase 1 prepare
func (p *Paxos) P1aTX(t int64) {

	if p.Active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.txTime = t
	m := Prepare{Key: p.Key, Table: *p.Table, Ballot: p.ballot, txTime:t, Try:p.Try}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

func (p *Paxos) P1aRetry(try int) {
	if p.Try > try {
		return
	} else {
		p.Try = try + 1
		p.P1aNoBallotIncrease()
	}
}

// P1a starts phase 1 prepare
func (p *Paxos) P1aNoBallotIncrease() {

	if p.Active {
		return
	}
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	m := Prepare{Key: p.Key, Table: *p.Table, Ballot: p.ballot, txTime: p.txTime, Try: p.Try}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *fleetdb.Request) {

	p.P2aFillSlot(r.Command, r, nil)
	m := Accept{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
		txtime: r.Timestamp,
		EpochSlot:p.epochSlot,
	}
	log.Debugf("Replica %s RBroadcast [%v]\n", p.ID(), m)
	p.RBroadcast(p.ID().Zone(), &m)
}

func (p *Paxos) P2aFillSlot(cmd key_value.Command, req *fleetdb.Request, tx *fleetdb.Transaction) {
	p.slot++
	e :=  &entry{
		ballot:    p.ballot,
		command:   cmd,
		request:   req,
		tx:        tx,
		quorum:    NewQuorum(),
	}
	if req != nil {
		e.timestamp = req.Timestamp
	}
	if tx != nil {
		e.timestamp = tx.Timestamp
	}
	p.log[p.slot] = e
	p.log[p.slot].quorum.ACK(p.ID())
}


func (p *Paxos) HandleP1a(m Prepare) {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	l := make(map[int]CommandBallot)
	//we send the log since last mutation to make sure we have correct value at the new leader
	for s := p.lastMutate; s <= p.slot; s++ {

		if p.log[s] == nil {
			continue
		}

		if p.log[s].command.Operation == key_value.TX_LEASE && !p.log[s].executed {
			// we are giving away unexecuted TX lease, so abort that TX
			// in essence we want ot sent the reply to channle waiting for all leases, so we do not block.
			// upon starting phase2, however the system will catch that we do not own an object and abort
			if p.log[s].request != nil {
				p.log[s].request.Reply(fleetdb.Reply{
					Command: p.log[s].command,
					Err:     ErrTXPreempted,
				})
				p.log[s].request = nil
			}
		}

		cb := CommandBallot{
			Command:	p.log[s].command,
			Ballot:		p.log[s].ballot,
			Executed: 	p.log[s].executed,
			Committed:	p.log[s].commit}

		if p.log[s].tx != nil {
			cb.Tx = *p.log[s].tx
			cb.HasTx = true
		} else {
			cb.HasTx = false
		}
		l[s] = cb

	}

	lpf := false
	lt := m.Try
	hasLease := p.HasTXLease(m.txTime)
	log.Debugf("Replica %s ===[%v]===>>> Replica %s {has lease = %t, bal=%v, p.slot=%d, p.exec=%d, log=%v}\n", m.Ballot.ID(), m, p.ID(), hasLease, p.ballot, p.slot, p.execute, l)
	// new leader
	if m.Ballot > p.ballot && !hasLease {
		//we have no active lease slot, so we ok to give up Key
		p.ballot = m.Ballot
		p.Active = false
		if len(p.requests) > 0 {
			defer p.P1a()
		}

	} else if hasLease {
		lpf = true //we reply with lpf flag to let know that rejection is due to lease
	}


	p.Send(m.Ballot.ID(), &Promise{
		Key: 		p.Key,
		Table:		*p.Table,
		Ballot: 	p.ballot,
		ID:     	p.ID(),
		LPF:		lpf,
		Try:		lt,
		Log:    	l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot, mID ids.ID) {
	//if we learn of any slots, see what we need to update
	if len(scb) > 0 {
		log.Debugf("Updating Paxos based on CommandBallots: %v\n", scb)
		slots := make([]int, 0)
		for s, _ := range scb {
			slots = append(slots, s)
		}
		sort.Ints(slots)
		//we set our execute counter to their if it is bigger
		//since we have not full replication, this node may not be aware of
		//all previous slots, but now it is becoming a leader and most continue
		//the log
		p.execute = utils.Max(p.execute, slots[0])
		p.epochSlot = p.execute
		log.Debugf("Updating slots: %v\n", slots)
		//Iterate in the slot order
		for _, s := range slots {
			cb := scb[s]
			p.slot = utils.Max(p.slot, s)
			if e, exists := p.log[s]; exists {
				if !e.commit && cb.Ballot > e.ballot {
					log.Debugf("Updating slot %d: %v\n", s, cb)
					e.ballot = cb.Ballot
					e.command = cb.Command
					e.commit = false
					if !cb.Executed {
						e.oldLeader = &mID
					}
					if !cb.Executed && cb.HasTx {
						e.tx = &cb.Tx
					} else {
						e.tx = nil
					}
				}
			} else {
				log.Debugf("Adding slot %d: %v\n", s, cb)
				e := &entry{
					ballot:  cb.Ballot,
					command: cb.Command,
					commit:  false,
				}
				if !cb.Executed {
					e.oldLeader = &mID
				}
				if !cb.Executed && cb.HasTx {
					e.tx = &cb.Tx
				} else {
					e.tx = nil
				}
				p.log[s] = e
			}
		}
		log.Debugf("Done Updating Paxos based on CommandBallots\n")
	}

}

// HandleP1b handles p1b message
func (p *Paxos) HandleP1b(m Promise) {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()
	// old message

	if m.Ballot < p.ballot || p.Active {

		if m.LPF {
			//we hit a lease, so retry later
			defer p.P1aRetry(m.Try)
		}

		log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	//log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	p.update(m.Log, m.ID)

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.Active = false // not necessary
		p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.quorum.Q1() {
			p.Try = 0
			p.Active = true
			// propose any uncommitted entries or entries since last mutate (including) onwards
			log.Debugf("Proposing learned entries for slots %d to %d:", p.execute, p.slot)
			for i := p.execute; i <= p.slot; i++ {

				if p.log[i] == nil || p.log[i].commit {
					log.Debugf("Proposing learned entry for slot %d [SKIP]", i)
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				m := Accept{
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command,
					EpochSlot:p.epochSlot,
				}
				log.Debugf("Replica %s RBroadcast [%v]\n", p.ID(), m)
				p.RBroadcast(p.ID().Zone(), &m)
			}
			// propose new commands
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]*fleetdb.Request, 0)
		}
	}
}

func (p *Paxos) HandleP2a(m Accept) {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	p.epochSlot = m.EpochSlot
	p.execute = utils.Max(p.execute, m.EpochSlot)
	p.ProcessP2a(m, nil)

	p.Send(m.Ballot.ID(), &Accepted{
		Key:	p.Key,
		Table:  *p.Table,
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

func (p *Paxos) ProcessP2a(m Accept, tx *fleetdb.Transaction) {
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.Active = false
		// update slot number
		p.slot = utils.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				log.Debugf("Replica %s SWAP SLOT ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Retry(*e.request)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
				e.tx = tx

			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  	m.Ballot,
				command: 	m.Command,
				tx:			tx,
				commit:  	false,
			}
		}
	}
}

func (p *Paxos) HandleP2b(m Accepted) bool {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	if slotEntry, exists := p.log[m.Slot]; exists {
		// old message
		if m.Ballot < slotEntry.ballot || slotEntry.commit {
			return false
		}
		// reject message
		if m.Ballot > p.ballot {
			p.ballot = m.Ballot
			p.Active = false
		}

		// ack message
		if m.Ballot.ID() == p.ID() && m.Ballot == slotEntry.ballot {
			slotEntry.quorum.ACK(m.ID)
			if slotEntry.quorum.Q2() {
				slotEntry.commit = true
				m := Commit{
					Slot:    m.Slot,
					Command: slotEntry.command,
				}
				p.RBroadcast(p.ID().Zone(), &m)
				log.Debugf("Replica %s RBroadcasted [%v]\n", p.ID(), m)
				if slotEntry.oldLeader != nil {
					//we have old leader on this lsot which may be waiting on the commit decision to reply
					//to its client
					log.Debugf("Replica %s Send [%v] to Old Leader $s \n", p.ID(), m, slotEntry.oldLeader)
					p.Send(*slotEntry.oldLeader, &m)
				}
				return true
			} else {
				log.Debugf("Replica %s NO Q2 [%v]\n", p.ID(), m)
			}
		}
	}
	return false
}

func (p *Paxos) HandleP2bTX(m Accepted) int  {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	if slotEntry, exists := p.log[m.Slot]; exists {
		// old message
		if m.Ballot < slotEntry.ballot || slotEntry.commit {
			return -2
		}

		// reject message
		if m.Ballot > p.ballot {
			p.ballot = m.Ballot
			p.Active = false
			return -1
		}

		// ack message
		if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
			p.log[m.Slot].quorum.ACK(m.ID)
			if p.log[m.Slot].quorum.Q2() {
				p.log[m.Slot].commit = true
				return 1
			}
		} else {
			return -1
		}
	} else {
		return -2 //not having a slot at this stage is like having an old msg
	}
	return 0
}

func (p *Paxos) HandleP3(m Commit) {
	p.GetAccessToken()
	//log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, p.ID())

	p.slot = utils.Max(p.slot, m.Slot)

	if e, exists := p.log[m.Slot]; exists {
		if !e.command.Equal(m.Command) && e.request != nil {
			p.Retry(*e.request)
			e.request = nil
		}
		// update command
		e.command = m.Command
		e.commit = true
	} else {
		p.log[m.Slot] = &entry{
			command: m.Command,
			commit:  true,
		}
	}
	p.Exec()
	p.ReleaseAccessToken()
}

func (p *Paxos) SlotNOOP(slot int) {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	p.log[slot].command.Operation = key_value.NOOP
	p.log[slot].commit = true
}

func (p *Paxos) Exec() {

	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			if e != nil {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, e=%v]\n", p.ID(), string(p.Key), p.execute, e)
			} else {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, No LOG ENTRY]\n", p.ID(), string(p.Key), p.execute)
			}
			break
		}

		log.Debugf("Replica %s execute s=%d for key=%v [e=%v]\n", p.ID(), p.execute, string(p.Key), e)

		if e.command.Operation == key_value.TX_LEASE  {
			txid := binary.LittleEndian.Uint64(e.command.Value)
			p.setLease(e.timestamp, ids.TXID(txid))
			if e.request != nil {
				e.request.Reply(fleetdb.Reply{
					Command: e.command,
				})
				e.request = nil
			}
			e.executed = true
			p.execute++
		} else {
			if e.tx == nil || e.command.Operation == key_value.NOOP {
				value, err := p.Execute(e.command)
				if err == nil {
					if e.request != nil {
						log.Debugf("setting reply: %s\n", value)
						e.request.Reply(fleetdb.Reply{
							Command: e.command,
							Value:   value,
						})

						e.request = nil
					}

				} else {
					//log.Errorln(err)
					if err != key_value.ErrNotFound {
						log.Errorf("Exec Error {entry = %v}: %s\n", e, err)
					}
					if e.request != nil {
						log.Debugf("reply with err: %s\n", err)
						e.request.Reply(fleetdb.Reply{
							Command: e.command,
							Err:	 err,
						})
						e.request = nil
					}
				}
				//Clean up the log after execute
				e.executed = true
				if value == nil && e.command.Operation != key_value.NOOP {
					p.lastMutate = p.execute
					p.cleanLog(p.execute - 1)
				}
				p.execute++
			} else {
				tx := p.GetTX(e.tx.TxID)
				log.Debugf("Ready to Exec TxID %v (key=%s, Table=%s)\n", e.tx.TxID, string(e.command.Key), e.command.Table)
				if tx == nil {
					log.Errorf("No TX found with TxID: %v", e.tx.TxID)
				}
				p.GetTX(e.tx.TxID).ReadyToExec(p.execute, e.command.Key)
				break // get out the loop and finish this Exec cycle
			}
		}
	}
}

func (p* Paxos) ExecTXCmd() key_value.Value {
	log.Debugf("PREP TO Execute CMD on slot %d\n", p.execute)
	e, ok := p.log[p.execute]
	if ok {
		log.Debugf("ABOUT TO Execute CMD %v\n", e.command)
		value, err := p.Execute(e.command)
		if err == nil {
			e.executed = true
			//Clean up the log after execute
			if value == nil && e.command.Operation != key_value.NOOP {
				p.lastMutate = p.execute
				p.cleanLog(p.execute - 1)
			}
			p.execute++
			if e.tx != nil { //I think we should always have tx here, but just in case
				p.resetLease(e.tx.TxID)
			}
			return value
		} else {
			log.Errorln(err)
			return nil
		}
	}
	return nil
}

func (p *Paxos) cleanLog(slotNum int) {
	for s, _ := range p.log {
		if s <= slotNum {
			delete(p.log, s)
		}
	}
	if len(p.log) == 0 {
		p.log = make(map[int]*entry, p.Config().BufferSize)
	}
	if len(p.log) == 1 {
		tempLog := make(map[int]*entry, p.Config().BufferSize)
		for s, _ := range p.log {
			tempLog[s] = p.log[s]
		}
		p.log = tempLog
	}
}

func (p *Paxos) HasTXLease(txtime int64) bool {
	if p.txLease != 0 {
		/*if p.txTime == 0 {
			log.Errorf("txtime is 0 in lease \n")
		}*/
		t := p.txLeaseStart.Add(p.txLeaseDuration)
		if t.After(time.Now()) && (p.txTime <= txtime || txtime == 0) {
			return true
		}
	}
	return false
}

func (p *Paxos) resetLease(txid ids.TXID) {
	if p.txLease == txid {
		log.Debugf("Removing Lease for key %v on TX %v", string(p.Key), txid)
		p.txLease = 0
		p.txTime = 0
		p.txLeaseStart = time.Unix(0, 0)
	}
}

func (p *Paxos) setLease(t int64, txid ids.TXID) {
	log.Debugf("Setting Lease for key %v on TX %v", string(p.Key), txid)
	p.txLease = txid
	p.txTime = t
	p.txLeaseStart = time.Now()
}