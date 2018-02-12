package wpaxos2

import (
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"time"
	"sync"
)


type Replica struct {
	fleetdb.Node
	paxi  map[string]*Paxos
	//TODO: clean up stats map when object no longer own the object
	//stats map[string] hitstat
	txs map[fleetdb.TXID] *fleetdb.Transaction //this is the map of all outstanding TX Replica knows of

	//Key fleetdb.Key // current working Key
	txl sync.RWMutex
	sync.RWMutex

}

func NewReplica(config fleetdb.Config) *Replica {
	r := new(Replica)
	r.Node = fleetdb.NewNode(config)
	r.paxi = make(map[string]*Paxos)
	//r.stats = make(map[string] hitstat)
	r.txs = make(map[fleetdb.TXID] *fleetdb.Transaction)

	//transaction
	r.Register(fleetdb.Transaction{}, r.HandleTransaction)
	//request
	r.Register(fleetdb.Request{}, r.HandleRequest)
	//wpaxos
	r.Register(Prepare{}, r.HandlePrepare)
	r.Register(Promise{}, r.handlePromise)
	r.Register(Accept{}, r.handleAccept)
	r.Register(AcceptTX{}, r.handleAcceptTX)
	r.Register(Accepted{}, r.handleAccepted)
	r.Register(AcceptedTX{}, r.handleAcceptedTX)
	r.Register(Commit{}, r.handleCommit)
	r.Register(CommitTX{}, r.handleCommitTX)

	return r
}

func (r *Replica) init(key fleetdb.Key) {
	r.Lock()
	defer r.Unlock()
	if _, exists := r.paxi[key.B64()]; !exists {
		log.Debugf("Init Paxos Replicata for Key %s\n", key)
		r.paxi[key.B64()] = NewPaxos(r, key)
		//r.stats[key.B64()] = newStat(r.Config().Interval)
	}
}

func (r *Replica) GetPaxos(key fleetdb.Key) *Paxos {
	r.RLock()
	defer r.RUnlock()

	return r.paxi[key.B64()]
}
/* ----------------------------------------------------------------------
 *
 *								Transactions
 *
 * ---------------------------------------------------------------------*/
func (r *Replica) HandleTransaction(m fleetdb.Transaction) {
	log.Debugf("Replica %s received TX %v {%v}\n", r.ID(), m.TxID, m)
	//first we check what Keys we have and run phase2-3 with TX_LEASE command
	//then we run phase-1 to get remaining keys
	//when we get they Key, we immediately run phase2-3 with TX_LEASE
	//once all keys are stolen we run phase2-3 to get the TX-values
	//if success we Commit
	//if not, we abort

	TxLeaseChan := make(chan fleetdb.Reply)
	go r.waitForLease(&m, TxLeaseChan, m.TxID)
	for _, c := range m.Commands {
		r.init(c.Key)
		p := r.GetPaxos(c.Key)

		r.Lock()
		//r.stats[c.Key.B64()].HitWeight(c.ClientID, len(m.Commands) / 2 + 1)
		r.Unlock()

		if p != nil && p.IsLeader() {
			r.sendLeaseP2a(c, p, m.Timestamp, TxLeaseChan)
		} else {
			//we need to steal
			r.sendTxP1a(c, p, m.Timestamp, TxLeaseChan)
		}
	}
}


func (r *Replica) createLeaseRequest(c fleetdb.Command, txtime int64, TxLeaseChan chan fleetdb.Reply) fleetdb.Request {
	cmdLease := new(fleetdb.Command)
	cmdLease.Key = c.Key
	//cmdLease.TxID = c.TxID
	cmdLease.Operation = fleetdb.TX_LEASE
	return fleetdb.Request{Command:*cmdLease, C:TxLeaseChan, Timestamp:txtime}
}

func (r *Replica) sendLeaseP2a(c fleetdb.Command, p *Paxos, t int64, TxLeaseChan chan fleetdb.Reply) {
	leaseReq := r.createLeaseRequest(c, t, TxLeaseChan)
	p.Lock()
	defer p.Unlock()

	p.P2a(&leaseReq)
}

func (r *Replica) sendTxP1a(c fleetdb.Command, p *Paxos, t int64, TxLeaseChan chan fleetdb.Reply) {
	leaseReq := r.createLeaseRequest(c, t, TxLeaseChan)
	p.Lock()
	defer p.Unlock()

	p.AddRequest(leaseReq)
	p.P1aTX(t)
}

func (r *Replica) waitForLease(m *fleetdb.Transaction, TxLeaseChan chan fleetdb.Reply, TxID fleetdb.TXID)  {
	log.Debugf("Replica %s waiting for TX lease {%v} %v\n", r.ID(), m.TxID, m)
	recvd := 0
	for recvd < len(m.Commands) {
		reply := <- TxLeaseChan
		log.Debugf("Replica %s received TX_LEASE reply %v\n", r.ID(), reply)
		recvd++
	}
	//now we can start p2a for all keys
	r.startTxP2a(m)
}

func (r *Replica) startTxP2a(tx *fleetdb.Transaction) {
	log.Debugf("Replica %s starting TX {%v} P2a %v\n", r.ID(), tx.TxID, tx)
	p2as := make([]Accept, len(tx.Commands))
	slots := make([]int, len(tx.Commands))

	for _, c := range tx.Commands {

		p := r.GetPaxos(c.Key)
		p.Lock()
		if p.Ballot().ID() != r.ID() {
			//there is a key we do not own
			//so we reject

			log.Debugf("Replica %s: startTxP2a TX {%v} REJECT SEND TO CLIENT %s Due to {key=%v, bal=%v}\n", r.ID(), tx.TxID, tx.ClientID, string(p.Key), p.Ballot())
			tx.Reply(fleetdb.TransactionReply{
				OK:        false,
				CommandID: tx.CommandID,
				ClientID:  tx.ClientID,
				Commands:  tx.Commands,
				Timestamp: time.Now().UnixNano(),
			})
			p.Unlock()
			return
		}
		p.Unlock()
	}

	for i, c := range tx.Commands {
		p := r.GetPaxos(c.Key)
		p.Lock()
		p.P2aFillSlot(c, nil, tx)
		slots[i] = p.SlotNum()
		p2as[i] = Accept{Key: p.Key, Ballot:p.Ballot(), Slot:p.SlotNum(), Command:c}
		p.Unlock()
	}

	acceptTx := new(AcceptTX)
	acceptTx.TxID = tx.TxID
	acceptTx.P2as = p2as
	acceptTx.LeaderID = r.ID()
	tx.MakeCommittedWaitingFlags(slots)
	r.txl.Lock()
	r.txs[acceptTx.TxID] = tx
	r.txl.Unlock()
	log.Debugf("Replica %s Rbroadcast Tx P2a [%v]\n", r.ID(), acceptTx)
	r.RBroadcast(r.ID().Zone(), &acceptTx)
}

func (r *Replica) handleAcceptTX(m AcceptTX) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.P2as[0].Ballot.ID(), m, r.ID())

	cmds := make([]fleetdb.Command, len(m.P2as))
	slots := make([]int, len(m.P2as))
	p2bs := make([]Accepted, len(m.P2as))
	keys := make([]fleetdb.Key, len(m.P2as))

	for i, p2a := range m.P2as {
		p := r.GetPaxos(p2a.Command.Key)

		p.Lock()
		p.ProcessP2a(p2a, true)
		p2bs[i] = Accepted{
			Key:	p.Key,
			Ballot: p.Ballot(),
			Slot:   p2a.Slot,
			ID:     p.ID(),
		}
		cmds[i] = p2a.Command
		keys[i] = p2a.Command.Key
		slots[i] = p2a.Slot
		p.Unlock()
	}
	//log.Debugf("Before r.Lock \n")
	r.Lock()
	//log.Debugf("After r.Lock\n")
	r.txs[m.TxID] = fleetdb.NewInProgressTX(m.TxID, cmds, slots)
	r.Unlock()
	accTx := new(AcceptedTX)
	accTx.P2bs = p2bs
	accTx.TxID = m.TxID
	log.Debugf("Replica %v ======{%v}=====> Replica %v \n", r.ID(), accTx, m.TxID.ID())
	r.Send(m.LeaderID, &accTx)
}

func (r *Replica) handleAcceptedTX(msg AcceptedTX) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", msg.P2bs[0].ID, msg, r.ID())
	r.txl.RLock()
	tx := r.txs[msg.TxID]
	r.txl.RUnlock()
	if tx != nil {
		for _, p2b := range msg.P2bs {
			p := r.GetPaxos(p2b.Key)
			p.Lock()
			result := p.HandleP2bTX(p2b)
			p.Unlock()
			log.Debugf("Replica %s: HandleP2bTx Result = %d\n", r.ID(), result)

			switch result {
			case -1:
				//this is bad, we need to abort TX
				r.startTxP3(msg.TxID, false)
				tx.P3Sent()
				if tx != nil {
					log.Debugf("Replica %s: HandleP2bTx TX {%v} REJECT SEND TO CLIENT %d\n", r.ID(), msg.TxID)
					tx.Reply(fleetdb.TransactionReply{
						OK:        false,
						CommandID: tx.CommandID,
						ClientID:  tx.ClientID,
						Commands:  tx.Commands,
						Timestamp: time.Now().UnixNano(),
					})
				}

			case 1:
				tx.MarkCommitted(p2b.Key)
			}
		}
		if tx.AreAllCommitted() && tx.CanSendP3() {
			//we have all keys with Q2s, we can send P3 Commit
			log.Debugf("Replica %s: starts TXP3 {txid=%v}\n", r.ID(), msg.TxID)
			r.startTxP3(msg.TxID, true)
			tx.P3Sent()
		}
	} else {
		log.Debugf("Replica %s: TX {txid=%v} is missing at Replica \n", r.ID(), msg.TxID)
	}

}

func (r *Replica) GetTX(txid fleetdb.TXID) *fleetdb.Transaction {
	r.RLock()
	defer r.RUnlock()
	return r.txs[txid]
}


func (r *Replica) startTxP3(txid fleetdb.TXID, commit bool) {
	r.txl.RLock()
	p3s := make([]Commit, len(r.txs[txid].CmdMeta))
	tx := r.txs[txid]
	r.txl.RUnlock()

	tx.MakeExecChannel()
	go r.ExecTx(tx)
	log.Debugf("Replica %s: Starting TX p3 {%v} commit = %t \n", r.ID(), tx, commit)
	for i, meta := range tx.CmdMeta {
		log.Debugf("Replica %s: i = %d \n", r.ID(), i)
		cmd := tx.Commands[i]
		p := r.GetPaxos(cmd.Key)
		if !commit {
			cmd.Operation = fleetdb.NOOP
			p.SlotNOOP(meta.Slot)
		}
		p3 := Commit{Key: cmd.Key, Slot: meta.Slot, Command: cmd}
		p3s[i] = p3

		if p != nil {
			log.Debugf("Replica %s: Attempt to Exec %v\n", r.ID(), string(cmd.Key))
			p.Exec()
		}
	}
	txCommit := CommitTX{TXID: txid, P3s:p3s}
	log.Debugf("Replica %s: Broadcast TX Commit %v\n", r.ID(), txid)
	r.Broadcast(&txCommit)

	if !commit {
		//we abort, so can delete this TX.
		//it will not execute as TX and instead individual NOOP will execute
		log.Debugf("Replica %s delete TX (Abort) %v\n", r.ID(), txid)
		r.txl.Lock()
		defer r.txl.Unlock()
		delete(r.txs, txid)
	}
}

func (r *Replica) handleCommitTX(m CommitTX) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.TXID, m, r.ID())
	//we commit
	r.txl.Lock()
	if r.txs[m.TXID] == nil {
		//we have not seen this TX yet
		cmds := make([]fleetdb.Command, len(m.P3s))
		slots := make([]int, len(m.P3s))
		for i, p3 := range m.P3s {
			slots[i] = p3.Slot
			cmds[i] = p3.Command
		}
		r.txs[m.TXID] = fleetdb.NewInProgressTX(m.TXID, cmds, slots)
	}
	r.txl.Unlock()
	for _, p3 := range m.P3s {
		k := p3.Command.Key
		r.init(k)
		r.handleCommit(p3)
	}
}

func (r *Replica) ExecTx(tx *fleetdb.Transaction) {

	log.Debugf("Replica %s waiting for TX EXEC {txid=%v}\n", r.ID(), tx.TxID)
	execReady := 0
	execChan := tx.GetExecChannel()
	for execReady < len(tx.Commands) {
		reply := <- execChan
		log.Debugf("Replica %s TX READY for key @ slot=%d %v\n", r.ID(), reply.Key, reply.Slotnum)
		execReady++
	}

	log.Debugf("Replica %s Execute TX %v\n", r.ID(), tx.TxID)


	//can execute TX now
	for _, cmd := range tx.Commands {
		p := r.GetPaxos(cmd.Key)
		//p.Lock()
		p.ExecTXCmd(cmd)
		//p.Unlock()
	}

	//reply if needed
	if tx != nil {
		log.Debugf("Replica %s replied to client for TX %v\n", r.ID(), tx.TxID)
		tx.Reply(fleetdb.TransactionReply{
			OK:true,
			CommandID: tx.CommandID,
			ClientID: tx.ClientID,
			Commands: tx.Commands,
			Timestamp: time.Now().UnixNano(),
		})
	}
	/*r.txl.RLock()
	inProgressTx := r.txs[txid]
	if inProgressTx == nil {
		log.Errorf("InProgressTX is nil: {txid = %v, key= %v}\n", txid, key)
		return false
	}
	r.txl.RUnlock()*/
	//inProgressTx.MarkWaiting(key)



	/*if inProgressTx.AreAllWaiting() {
		//can execute TX now
		for _, cmd := range inProgressTx.Commands {
			p := r.GetPaxos(cmd.Key)
			//p.Lock()
			p.ExecTXCmd(cmd)
			//p.Unlock()
		}
		//reply if needed
		if tx != nil {
			log.Debugf("Replica %s replied to client for TX %v\n", r.ID(), txid)
			tx.Reply(fleetdb.TransactionReply{
				OK:true,
				CommandID: tx.CommandID,
				ClientID: tx.ClientID,
				Commands: tx.Commands,
				Timestamp: time.Now().UnixNano(),
			})
		}
		//remote tx from map of transactions
		log.Debugf("Replica %s delete TX %v\n", r.ID(), txid)
		r.txl.Lock()
		defer r.txl.Unlock()
		delete(r.txs, txid)
		return true
	} else {
		//need to wait for other keys
		log.Debugf("Need to Wait for Other keys in TX %v\n", txid)
		return false
	}*/
}


func (r *Replica) processLeaderChange(to fleetdb.ID, p *Paxos) {
	if to.Zone() != r.ID().Zone() {
		//we are changing zone.
		p.Send(to, &LeaderChange{
			Key:    p.Key,
			To:     to,
			From:   r.ID(),
			Ballot: p.Ballot(),
		})
	}
}

/* ----------------------------------------------------------------------
 *
 *								Normal Requests
 *
 * ---------------------------------------------------------------------*/

//Request
func (r *Replica) HandleRequest(m fleetdb.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	k := m.Command.Key
	r.init(k)
	p := r.GetPaxos(k)
	p.HandleRequest(m)
}



//WPaxos
func (r *Replica) HandlePrepare(m Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	p := r.GetPaxos(m.Key)
	//p.Lock()
	p.HandleP1a(m)
	//p.Unlock()
}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	p := r.GetPaxos(m.Key)
	//p.Lock()
	p.HandleP1b(m)
	//p.Unlock()
}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key)
	p := r.GetPaxos(m.Key)
	//p.Lock()
	p.HandleP2a(m)
	//p.Unlock()
}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	p := r.GetPaxos(m.Key)
	//p.Lock()
	needExec := p.HandleP2b(m)
	log.Debugf("handleAccepted needExec %s\n", needExec)
	//p.Unlock()
	if needExec {
		p.Exec()
	}

}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, r.ID())
	r.init(m.Key)
	p := r.GetPaxos(m.Key)
	//p.Lock()
	p.HandleP3(m)
	//p.Unlock()
}


func (r *Replica) CountKeys() int {
	sum := 0
	r.RLock()
	defer r.RUnlock()

	for _, paxos := range r.paxi {
		if paxos.Active {
			sum++
		}
	}

	return sum
}

