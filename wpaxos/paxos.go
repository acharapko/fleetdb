package wpaxos

import (
	"time"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"fmt"
	"encoding/binary"
	//"runtime/debug"
)

// entry in log
type entry struct {
	ballot    fleetdb.Ballot
	command   fleetdb.Command
	commit    bool
	executed  bool

	tx        *fleetdb.Transaction
	request   *fleetdb.Request

	quorum    *fleetdb.Quorum
	timestamp int64
}

func (e entry) String() string {
	return fmt.Sprintf("Slot Entry {bal=%s, cmd=%v, commit=%v, exec=%v, tx=%v}", e.ballot, e.command, e.commit, e.executed, e.tx)
}

// Paxos instance
type Paxos struct {
	fleetdb.Node

	Key 			fleetdb.Key

	log     		map[int]*entry  // log ordered by slot
	execute 		int             // next execute slot number
	Active  		bool            // Active leader
	Try		 		int			//Try Number for P1a
	ballot  		fleetdb.Ballot // highest ballot number
	slot    		int             // highest slot number

	txLeaseDuration time.Duration
	txLeaseStart    time.Time
	txTime			int64
	txLease			fleetdb.TXID

	quorum   		*fleetdb.Quorum    // phase 1 quorum
	requests 		[]*fleetdb.Request // phase 1 pending requests

	//lockNum	int

	//sync.RWMutex //Lock to control concurrent access to the same paxos instance

	Token 			chan bool

}

// NewPaxos creates new paxos instance
func NewPaxos(n fleetdb.Node, key fleetdb.Key) *Paxos {
	plog := make(map[int]*entry, n.Config().BufferSize)
	//log[0] = &entry{}
	p := &Paxos{
		Node:            n,
		log:             plog,
		execute:         1,
		Key:             key,
		txLeaseDuration: time.Duration(n.Config().TX_lease) * time.Millisecond,
		quorum:          fleetdb.NewQuorum(),
		requests:        make([]*fleetdb.Request, 0),
		Token:			 make(chan bool, 1),
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

func (p *Paxos) GetAccessToken() bool {
	t := <- p.Token
	//log.Debugf("Acquired Token %v-%d\n", string(p.Key), t)
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
	m := Prepare{Key: p.Key, Ballot: p.ballot, Try:p.Try}
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
	m := Prepare{Key: p.Key, Ballot: p.ballot, txTime:t, Try:p.Try}
	log.Debugf("Replica %s broadcast [%v]\n", p.ID(), m)
	p.Broadcast(&m)
}

func (p *Paxos) P1aRetry(try int) {
	if p.Try < try {
		return
	} else {
		p.Try = try
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
	m := Prepare{Key: p.Key, Ballot: p.ballot, Try: p.Try}
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
	//p.execute is nxt slot to execute, but we need to send last executed slot
	for s := p.execute - 1; s <= p.slot; s++ {

		if p.log[s] == nil {
			continue
		}

		cb := CommandBallot{
			Command:	p.log[s].command,
			Ballot:		p.log[s].ballot,
			Executed: 	p.log[s].executed,
			Committed:	p.log[s].commit}

		if p.log[s].tx != nil {
			cb.Tx = *p.log[s].tx
		}
		l[s] = cb

	}

	lpf := false
	lt := m.Try
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
		Key: 		p.Key,
		Ballot: 	p.ballot,
		ID:     	p.ID(),
		LPF:		lpf,
		Try:		lt,
		Log:    	l,
	})
}

func (p *Paxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = fleetdb.Max(p.slot, s)

		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
				e.commit = cb.Committed
				e.tx = &cb.Tx
				p.forceExec(cb, s)
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  cb.Committed,
				tx:		 &cb.Tx,
			}
			p.forceExec(cb, s)
		}
	}
}

func (p* Paxos) forceExec(cb CommandBallot, slot int) {
	if cb.Executed && p.execute < slot {
		log.Debugf("Replica %s forcing execution of %v\n", p.ID(), cb)
		p.execute = slot
		p.Exec()
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
			p.Try = 0
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
	p.GetAccessToken()
	defer p.ReleaseAccessToken()
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	p.ProcessP2a(m, nil)

	p.Send(m.Ballot.ID(), &Accepted{
		Key:	p.Key,
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
					Key:     p.Key,
					Slot:    m.Slot,
					Command: slotEntry.command,
				}
				p.RBroadcast(p.ID().Zone(), &m)
				log.Debugf("Replica %s RBroadcasted [%v]\n", p.ID(), m)
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

	p.slot = fleetdb.Max(p.slot, m.Slot)

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
	p.ReleaseAccessToken()
	p.Exec()
}

func (p *Paxos) SlotNOOP(slot int) {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	p.log[slot].command.Operation = fleetdb.NOOP
	p.log[slot].commit = true
}

func (p *Paxos) Exec() {
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	for {
		//p.RLock()
		e, ok := p.log[p.execute]
		//p.RUnlock()
		if !ok || !e.commit {
			if e != nil {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, e=%v]\n", p.ID(), string(p.Key), p.execute, e)
			} else {
				log.Debugf("Replica %s {key=%v} SKIP execute [s=%d, No LOG ENTRY]\n", p.ID(), string(p.Key), p.execute)
			}
			break
		}

		log.Debugf("Replica %s execute s=%d [e=%v]\n", p.ID(), p.execute, e)

		if e.command.Operation == fleetdb.TX_LEASE  {
			//p.Lock()
			txid := binary.LittleEndian.Uint64(e.command.Value)
			p.setLease(e.timestamp, fleetdb.TXID(txid))
			if e.request != nil {
				e.request.Reply(fleetdb.Reply{
					Command: e.command,
				})
				e.request = nil
			}
			e.executed = true
			delete(p.log, p.execute)
			p.execute++
			//p.Unlock()
		} else {
			if e.tx == nil || e.command.Operation == fleetdb.NOOP {
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
				e.executed = true
				p.cleanLog(p.execute)
				p.execute++
			} else {
				p.GetTX(e.tx.TxID).ReadyToExec(p.execute, e.command.Key)
				break // get out the loop and finish this Exec cycle
			}
		}
	}
}

func (p* Paxos) ExecTXCmd() fleetdb.Value {
	log.Debugf("PREP TO Execute CMD on slot %d\n", p.execute)
	e, ok := p.log[p.execute]
	if ok {
		log.Debugf("ABOUT TO Execute CMD %v\n", e.command)
		value, err := p.Execute(e.command)
		if err == nil {
			e.executed = true
			//Clean up the log after execute
			p.cleanLog(p.execute)
			p.execute++
			if e.tx != nil { //I think we should always have tx here, ut just in case
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
	delete(p.log, slotNum)
	if len(p.log) == 0 {
		p.log = make(map[int]*entry, p.Config().BufferSize)
	}
}

func (p *Paxos) HasTXLease(txtime int64) bool {
	if p.txLease != 0 {
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

func (p *Paxos) resetLease(txid fleetdb.TXID) {
	if p.txLease == txid {
		p.txLease = 0
		p.txTime = 0
		p.txLeaseStart = time.Unix(0, 0)
	}
}

func (p *Paxos) setLease(t int64, txid fleetdb.TXID) {
	p.txLease = txid
	p.txTime = t
	p.txLeaseStart = time.Now()
}