package wpaxos2

import (
	"time"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"sync"
	"fmt"
)

// entry in log
type entry struct {
	ballot    fleetdb.Ballot
	command   fleetdb.Command
	commit    bool
	executed  bool

	TxID	  fleetdb.TXID
	isTxSlot  bool
	txwait    bool
	txtime    int64
	tx        *fleetdb.Transaction

	request   *fleetdb.Request
	quorum    *fleetdb.Quorum
	timestamp time.Time
}

func (e entry) String() string {
	return fmt.Sprintf("Slot Entry {bal=%s, cmd=%v, commit=%v, exec=%v, istxslot =%t, txwait=%v, txtime=%d}", e.ballot, e.command, e.commit, e.executed, e.isTxSlot, e.txwait, e.txtime)
}

// Paxos instance
type Paxos struct {
	fleetdb.Node

	Key fleetdb.Key

	log     map[int]*entry  // log ordered by slot
	execute int             // next execute slot number
	Active  bool            // Active leader
	ballot  fleetdb.Ballot // highest ballot number
	slot    int             // highest slot number

	txLeaseDuration time.Duration
	txLeaseStart    time.Time
	txTime			int64
	txLease			bool

	quorum   *fleetdb.Quorum    // phase 1 quorum
	requests []*fleetdb.Request // phase 1 pending requests

	sync.RWMutex //Lock to control concurrent access to the same paxos instance

}

// NewPaxos creates new paxos instance
func NewPaxos(n fleetdb.Node, key fleetdb.Key) *Paxos {
	log := make(map[int]*entry, n.Config().BufferSize)
	//log[0] = &entry{}
	return &Paxos{
		Node:            n,
		log:             log,
		execute:         1,
		Key:             key,
		txLeaseDuration: time.Duration(n.Config().TX_lease) * time.Millisecond,
		quorum:          fleetdb.NewQuorum(),
		requests:        make([]*fleetdb.Request, 0),
	}
}

// IsLeader indicates if this node is current leader
func (p *Paxos) IsLeader() bool {
	return p.Active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *Paxos) Leader() fleetdb.ID {
	return p.ballot.ID()
}

// ballot returns current ballot
func (p *Paxos) Ballot() fleetdb.Ballot {
	return p.ballot
}

// SlotNum returns current ballot
func (p *Paxos) SlotNum() int {
	return p.slot
}


// HandleRequest handles request and start phase 1 or phase 2
func (p *Paxos) HandleRequest(r fleetdb.Request) {
	p.Lock()
	defer p.Unlock()
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
	m := Prepare{Key: p.Key, Ballot: p.ballot}
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
	m := Prepare{Key: p.Key, Ballot: p.ballot, txTime:t}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

// P1a starts phase 1 prepare
func (p *Paxos) P1aNoBallotIncrease() {

	if p.Active {
		return
	}
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	m := Prepare{Key: p.Key, Ballot: p.ballot}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

// P2a starts phase 2 accept
func (p *Paxos) P2a(r *fleetdb.Request) {

	p.P2aFillSlot(r.Command, r, nil)
	m := Accept{
		Key: p.Key,
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
		txtime: r.Timestamp,
	}
	log.Debugf("Replica %s RBroadcast [%v]\n", p.ID(), m)
	p.RBroadcast(p.ID().Zone(), &m)
}

func (p *Paxos) P2aFillSlot(cmd fleetdb.Command, req *fleetdb.Request, tx *fleetdb.Transaction) {
	p.slot++
	e :=  &entry{
		ballot:    p.ballot,
		command:   cmd,
		request:   req,
		tx:        tx,
		quorum:    fleetdb.NewQuorum(),
		timestamp: time.Now(),
	}
	if req != nil {
		e.txtime = req.Timestamp
	}
	if tx != nil {
		e.TxID = tx.TxID
		e.isTxSlot = true
		e.txtime = tx.Timestamp
	}
	p.log[p.slot] = e
	p.log[p.slot].quorum.ACK(p.ID())
}


func (p *Paxos) HandleP1a(m Prepare) {
	p.Lock()
	defer p.Unlock()

	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		/*if p.log[s] != nil && !p.log[s].commit && p.log[s].isTxSlot {
			//uncommitted TX slot, we treat as NOOP at this point
			p.log[s].command.Operation = fleetdb.NOOP
			p.log[s].txwait = false
		}*/
		//TODO: HandleP1a needs to change when not doing full replication
		//We should send all committed but not exec TX slots and all non-committed and non-executed regular slots.
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}

	lpf := false
	hasLease := p.HasTXLease(m.txTime)
	log.Debugf("Replica %s ===[%v]===>>> Replica %s {has lease = %b, bal=%v, p.slot=%d, p.exec=%d}\n", m.Ballot.ID(), m, p.ID(), hasLease, p.ballot, p.slot, p.execute)
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
		Key: 	p.Key,
		Ballot: p.ballot,
		ID:     p.ID(),
		LPF:	lpf,
		Log:    l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = fleetdb.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			}
		}
	}
}

// HandleP1b handles p1b message
func (p *Paxos) HandleP1b(m Promise) {
	p.Lock()
	defer p.Unlock()
	// old message
	if m.Ballot < p.ballot || p.Active {

		if m.LPF {
			//we hit a lease, so retry later
			defer p.P1aNoBallotIncrease()
		}

		log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}

	//log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	p.update(m.Log)

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
			p.Active = true
			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				// TODO nil gap?
				/*if p.log[i] != nil && !p.log[i].commit && p.log[i].isTxSlot {
					//uncommitted TX slot, we treat as NOOP at this point
					p.log[i].command.Operation = fleetdb.NOOP
					p.log[i].txwait = false
				}*/
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = fleetdb.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				m := Accept{
					Key:	 p.Key,
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command,
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
	p.Lock()
	defer p.Unlock()
	// log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ballot.ID(), m, p.ID())

	p.ProcessP2a(m, false)

	p.Send(m.Ballot.ID(), &Accepted{
		Key:	p.Key,
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     p.ID(),
	})
}

func (p *Paxos) ProcessP2a(m Accept, txSlot bool) {
	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.Active = false
		// update slot number
		p.slot = fleetdb.Max(p.slot, m.Slot)
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
				e.txtime = m.txtime
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  	m.Ballot,
				command: 	m.Command,
				isTxSlot:	txSlot,
				txtime:     m.txtime,
				commit:  	false,
			}
		}
	}
}

func (p *Paxos) HandleP2b(m Accepted) bool {
	p.Lock()
	defer p.Unlock()

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
					Key:     p.Key,
					Slot:    m.Slot,
					Command: slotEntry.command,
				}
				p.Broadcast(&m)
				log.Debugf("Replica %s broadcasted [%v]\n", p.ID(), m)
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

	// old message
	if m.Ballot < p.log[m.Slot].ballot || p.log[m.Slot].commit {
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
	return 0
}

func (p *Paxos) HandleP3(m Commit) {
	p.Lock()
	//log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, p.ID())

	p.slot = fleetdb.Max(p.slot, m.Slot)

	if e, exists := p.log[m.Slot]; exists {
		if !e.command.Equal(m.Command) && e.request != nil {
			p.Retry(*e.request)
			e.request = nil
		}
		// update command
		e.command = m.Command
		e.txwait = false
		e.commit = true
	} else {
		p.log[m.Slot] = &entry{
			command: m.Command,
			commit:  true,
		}
	}
	p.Unlock()
	p.Exec()
}

func (p *Paxos) SlotNOOP(slot int) {
	p.Lock()
	defer p.Unlock()

	p.log[slot].command.Operation = fleetdb.NOOP
	p.log[slot].commit = true
}

func (p *Paxos) Exec() {
	for {
		p.RLock()
		e, ok := p.log[p.execute]
		p.RUnlock()
		if !ok || !e.commit || e.txwait {
			if e != nil {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, e=%v]\n", p.ID(), string(p.Key), p.execute, e)
			} else {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, No LOG ENTRY]\n", p.ID(), string(p.Key), p.execute)
			}
			break
		}

		log.Debugf("Replica %s execute s=%d [e=%v]\n", p.ID(), p.execute, e)

		if e.command.Operation == fleetdb.TX_LEASE  {
			p.Lock()
			p.setLease(e.txtime)
			if e.request != nil {
				e.request.Reply(fleetdb.Reply{
					Command: e.command,
				})
				e.request = nil
			}
			e.executed = true
			delete(p.log, p.execute)
			p.execute++
			p.Unlock()
		} else {
			if e.TxID == 0 || e.command.Operation == fleetdb.NOOP {
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
					if err != fleetdb.ErrNotFound {
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
				p.Lock()
				e.executed = true
				p.cleanLog(p.execute)
				p.execute++
				p.Unlock()
			} else {
				//we have Tx command
				//we can execute it only when all other Tx Commands are committed
				//p.Unlock()
				//res := p.ExecTx(e.TxID, e.command.Key, e.tx)
				//p.Lock()
				//if !res {
					e.txwait = true //this will block exec further untill TX slot is out of the way
				//} e.TxID
				p.GetTX(e.TxID).ReadyToExec(p.execute, e.command.Key)
			}
		}
	}
}

func (p* Paxos) ExecTXCmd(cmd fleetdb.Command) fleetdb.Value {
	log.Debugf("Execute CMD %v\n", cmd)
	value, err := p.Execute(cmd)
	if err == nil {
		p.Lock()
		e, ok := p.log[p.execute]
		if ok {
			e.executed = true
		}
		//Clean up the log after execute
		p.cleanLog(p.execute)
		p.execute++
		p.resetLease()
		p.Unlock()
		return value
	} else {
		log.Errorln(err)
		return nil
	}
}

func (p *Paxos) cleanLog(slotNum int) {
	delete(p.log, slotNum)
	if len(p.log) == 0 {
		p.log = make(map[int]*entry, p.Config().BufferSize)
	}
}

func (p *Paxos) HasTXLease(txtime int64) bool {
	if p.txLease {
		/*if p.txTime == 0 {
			log.Errorf("txtime is 0 in lease \n")
		}*/
		t := p.txLeaseStart.Add(p.txLeaseDuration)
		if t.After(time.Now()) && p.txTime <= txtime {
			return true
		}
	}
	return false
}

func (p *Paxos) resetLease() {
	p.txLease = false
	p.txTime = 0
	p.txLeaseStart = time.Unix(0, 0)
}

func (p *Paxos) setLease(t int64) {
	p.txLease = true
	p.txTime = t
	p.txLeaseStart = time.Now()
}