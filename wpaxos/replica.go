package wpaxos

import (
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"time"
	"sync"
	"encoding/binary"
	"github.com/acharapko/fleetdb/key_value"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
	"github.com/acharapko/fleetdb/utils"
)

var (
	Migration_majority float64
)

type Replica struct {
	fleetdb.Node

	sync.RWMutex
	tables  map[string]*Table

	txl 	sync.RWMutex
	txs		map[ids.TXID] *fleetdb.Transaction //this is the map of all outstanding TX Replica knows of

	txexec sync.RWMutex
}

func NewReplica(config config.Config) *Replica {
	r := new(Replica)
	r.Node = fleetdb.NewNode(config)
	//r.paxi = make(map[string]*Paxos)
	r.tables = make(map[string]*Table)
	r.txs = make(map[ids.TXID] *fleetdb.Transaction)
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
	r.Register(Exec{}, r.handleExec)
	r.Register(CommitTX{}, r.handleCommitTX)

	zones := make(map[int]int)
	for id := range config.Addrs {
		zones[id.Zone()]++
	}

	NumZones = len(zones)
	NumNodes = len(config.Addrs)
	NumLocalNodes = zones[config.ID.Zone()]
	Migration_majority = config.Migration_maj
	F = config.F
	QuorumType = config.Quorum

	return r
}

func (r *Replica) init(key key_value.Key, table string) {
	r.Lock()
	defer r.Unlock()
	if _, exists := r.tables[table]; !exists {
		r.tables[table] = NewTable(table)
	}
	r.tables[table].Init(key, r)
}

func (r *Replica) GetPaxos(key key_value.Key, table string) *Paxos {
	if _, exists := r.tables[table]; exists {
		r.init(key, table)
		r.Lock()
		defer r.Unlock()
		return r.tables[table].GetPaxos(key)
	} else {
		r.init(key, table)
		r.Lock()
		defer r.Unlock()
		return r.tables[table].GetPaxos(key)
	}
}

func (r *Replica) GetPaxosByCmd(cmd key_value.Command) *Paxos {
	return r.GetPaxos(cmd.Key, cmd.Table)
}

func (r *Replica) GetTable(tableName string) *Table {
	r.Lock()
	defer r.Unlock()

	tbl := r.tables[tableName]
	if tbl == nil {
		r.tables[tableName] = NewTable(tableName)
		tbl = r.tables[tableName]
	}
	return tbl
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

	r.txl.Lock()
	r.txs[m.TxID] = &m
	r.txl.Unlock()

	TxLeaseChan := make(chan fleetdb.Reply)
	go r.waitForLease(&m, TxLeaseChan, m.TxID)
	for _, c := range m.Commands {
		r.init(c.Key, c.Table)
		p := r.GetPaxosByCmd(c)

		/*r.Lock()
		r.stats[c.Key.B64()].HitWeight(c.ClientID, len(m.Commands) / 2 + 1)
		r.Unlock()*/

		if p != nil && p.IsLeader() {
			r.sendLeaseP2a(c, p, m, TxLeaseChan)
		} else {
			//we need to steal
			r.sendTxP1a(c, p, m, TxLeaseChan)
		}
	}
}

func (r *Replica) createLeaseRequest(c key_value.Command, tx fleetdb.Transaction, TxLeaseChan chan fleetdb.Reply) fleetdb.Request {
	cmdLease := new(key_value.Command)
	cmdLease.Key = c.Key
	cmdLease.Table = c.Table

	//put tx id in the value
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(tx.TxID))
	cmdLease.Value = b

	cmdLease.Operation = key_value.TX_LEASE
	return fleetdb.Request{Command:*cmdLease, C:TxLeaseChan, Timestamp:tx.Timestamp}
}

func (r *Replica) sendLeaseP2a(c key_value.Command, p *Paxos, tx fleetdb.Transaction, TxLeaseChan chan fleetdb.Reply) {
	leaseReq := r.createLeaseRequest(c, tx, TxLeaseChan)
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	p.P2a(&leaseReq)
}

func (r *Replica) sendTxP1a(c key_value.Command, p *Paxos, tx fleetdb.Transaction, TxLeaseChan chan fleetdb.Reply) {
	leaseReq := r.createLeaseRequest(c, tx, TxLeaseChan)
	p.GetAccessToken()
	defer p.ReleaseAccessToken()

	p.AddRequest(leaseReq)
	p.P1aTX(tx.Timestamp)
}

func (r *Replica) waitForLease(m *fleetdb.Transaction, TxLeaseChan chan fleetdb.Reply, TxID ids.TXID)  {
	log.Debugf("Replica %s waiting for TX lease {%v} %v\n", r.ID(), m.TxID, m)
	recvd := 0
	for recvd < len(m.Commands) {
		reply := <- TxLeaseChan
		log.Debugf("Replica %s received TX_LEASE reply (TxID=%v) %v\n", r.ID(), TxID, reply)
		recvd++
		if reply.Err != nil {
			log.Debugf("Wait for Lease error (TxID=%v): %v\n", TxID, reply.Err)
		}
	}
	//now we can start p2a for all keys
	r.startTxP2a(m)
}

func (r *Replica) startTxP2a(tx *fleetdb.Transaction) {
	log.Debugf("Replica %s starting TX {%v} P2a %v\n", r.ID(), tx.TxID, tx)
	numKeys := len(tx.Commands)
	p2as := make([]Accept, numKeys)
	slots := make([]int, numKeys)

	tx.MakeExecChannel(numKeys)

	//first we lock this replica from making sure another TX does not try to get individual paxos boxes
	r.txexec.Lock()
	//lock all Paxos for the TX
	for _, c := range tx.Commands {
		p := r.GetPaxosByCmd(c)
		p.GetAccessToken()
	}
	proceedOk := true
	for _, c := range tx.Commands {
		p := r.GetPaxos(c.Key, c.Table)
		if p.Ballot().ID() != r.ID() || !p.Active {
			proceedOk = false
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
			break
		}
	}

	if proceedOk {
		for i, c := range tx.Commands {
			p := r.GetPaxosByCmd(c)
			p.P2aFillSlot(c, nil, tx)
			slots[i] = p.SlotNum()
			p2as[i] = Accept{Ballot: p.Ballot(), Slot: p.SlotNum(), Command: c}
		}
	}

	//unlock all paxos fot TX
	for _, c := range tx.Commands {
		p := r.GetPaxos(c.Key, c.Table)
		p.ReleaseAccessToken()
	}
	//Unlock tx Exec lock
	r.txexec.Unlock()

	if !proceedOk {
		return
	}

	acceptTx := new(AcceptTX)
	acceptTx.TxID = tx.TxID
	acceptTx.P2as = p2as
	acceptTx.LeaderID = r.ID()
	tx.MakeCommittedWaitingFlags(slots)

	log.Debugf("Replica %s Rbroadcast Tx P2a [%v]\n", r.ID(), acceptTx)
	r.RBroadcast(r.ID().Zone(), &acceptTx)
}

func (r *Replica) handleAcceptTX(m AcceptTX) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.P2as[0].Ballot.ID(), m, r.ID())

	cmds := make([]key_value.Command, len(m.P2as))
	slots := make([]int, len(m.P2as))
	p2bs := make([]Accepted, len(m.P2as))
	keys := make([]key_value.Key, len(m.P2as))

	for i, p2a := range m.P2as {
		slots[i] = p2a.Slot
		cmds[i] = p2a.Command
		keys[i] = p2a.Command.Key
	}
	tx := fleetdb.NewInProgressTX(m.TxID, cmds, slots)

	for i, p2a := range m.P2as {
		p := r.GetPaxosByCmd(p2a.Command)

		p.GetAccessToken()

		p.ProcessP2a(p2a, tx)
		p2bs[i] = Accepted{
			Key:	p.Key,
			Table:  *p.Table,
			Ballot: p.Ballot(),
			Slot:   p2a.Slot,
			ID:     p.ID(),
		}

		p.ReleaseAccessToken()
	}

	r.txl.Lock()
	r.txs[m.TxID] = tx
	r.txl.Unlock()
	go r.ExecTx(tx)


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
			p := r.GetPaxos(p2b.Key, p2b.Table)
			p.GetAccessToken()
			result := p.HandleP2bTX(p2b)
			p.ReleaseAccessToken()
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

func (r *Replica) GetTX(txid ids.TXID) *fleetdb.Transaction {
	r.txl.RLock()
	defer r.txl.RUnlock()
	return r.txs[txid]
}


func (r *Replica) startTxP3(txid ids.TXID, commit bool) {
	r.txl.RLock()
	numKeys := len(r.txs[txid].CmdMeta)
	p3s := make([]Commit, numKeys)
	tx := r.txs[txid]
	r.txl.RUnlock()

	go r.ExecTx(tx)
	log.Debugf("Replica %s: Starting TX p3 {%v} commit = %t \n", r.ID(), tx, commit)
	for i, meta := range tx.CmdMeta {
		//log.Debugf("Replica %s: i = %d \n", r.ID(), i)
		cmd := tx.Commands[i]
		p := r.GetPaxosByCmd(cmd)
		if !commit {
			cmd.Operation = key_value.NOOP
			p.SlotNOOP(meta.Slot)
		}
		p3 := Commit{Slot: meta.Slot, Command: cmd}
		p3s[i] = p3

		if p != nil {
			log.Debugf("Replica %s: Attempt to Exec %v\n", r.ID(), string(cmd.Key))
			p.GetAccessToken()
			p.Exec()
			p.ReleaseAccessToken()
		}
	}
	txCommit := CommitTX{TXID: txid, P3s:p3s}
	log.Debugf("Replica %s: RBroadcast TX Commit %v\n", r.ID(), txid)
	//r.Broadcast(&txCommit)
	r.RBroadcast(r.ID().Zone(), &txCommit)

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
		cmds := make([]key_value.Command, len(m.P3s))
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
		r.init(k, p3.Command.Table)
		r.handleCommit(p3)
	}
}

func (r *Replica) ExecTx(tx *fleetdb.Transaction) {

	log.Debugf("Replica %s waiting for TX EXEC {txid=%v}\n", r.ID(), tx.TxID)
	execChan := tx.GetExecChannel()
	execsReady := make(map[string] fleetdb.TxExec, len(tx.Commands))

	for len(execsReady) < len(tx.Commands) {
		reply := <- execChan
		ks := reply.Key.B64()
		if _, duplicate := execsReady[ks]; !duplicate {
			execsReady[ks] = reply
		}
		log.Debugf("Replica %s TX %v READY for key %v @ slot=%d\n", r.ID(), tx.TxID, string(reply.Key), reply.Slotnum)
	}

	log.Debugf("Replica %s Execute TX %v (%d commands)\n", r.ID(), tx.TxID, len(tx.Commands))

	//first we lock this replica from making sure another TX does not try to get individual paxos boxes
	r.txexec.Lock()
	//get all locks
	for _, cmd := range tx.Commands {
		p := r.GetPaxosByCmd(cmd)
		p.GetAccessToken()
	}

	//can execute TX now
	for _, cmd := range tx.Commands {
		p := r.GetPaxosByCmd(cmd)
		p.ExecTXCmd()
	}

	//reply if needed
	log.Debugf("Replica %s replied to client for TX %v\n", r.ID(), tx.TxID)
	tx.Reply(fleetdb.TransactionReply{
		OK:true,
		CommandID: tx.CommandID,
		ClientID: tx.ClientID,
		Commands: tx.Commands,
		Timestamp: time.Now().UnixNano(),
	})


	//can now try to execute next slots, as there may be some that waited for this TX
	for _, cmd := range tx.Commands {
		p := r.GetPaxosByCmd(cmd)
		p.Exec()
		p.ReleaseAccessToken() //and give up all locks
	}
	//and unlock tx execution lock
	r.txexec.Unlock()

	tx.CloseExecChannel()
}



/* ----------------------------------------------------------------------
 *
 *								Normal Requests
 *
 * ---------------------------------------------------------------------*/

//Request
func (r *Replica) HandleRequest(m fleetdb.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	t := m.Command.Table
	k := m.Command.Key
	r.init(k, t)
	p := r.GetPaxosByCmd(m.Command)
	p.HandleRequest(m)
}

//WPaxos
func (r *Replica) HandlePrepare(m Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Key, m.Table)
	p := r.GetPaxos(m.Key, m.Table)

	p.HandleP1a(m)

}

func (r *Replica) handlePromise(m Promise) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	p := r.GetPaxos(m.Key, m.Table)

	//find if any of the items belong to unfinished TX
	for _, e := range m.Log {
		if e.HasTx && !e.Executed {
			//add this TX to the list of TX, as we have to recover
			r.txl.Lock()
			r.txs[e.Tx.TxID] = &e.Tx
			r.txl.Unlock()
		}
	}

	p.HandleP1b(m)

}

func (r *Replica) handleAccept(m Accept) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	r.init(m.Command.Key, m.Command.Table)
	p := r.GetPaxosByCmd(m.Command)

	p.HandleP2a(m)

}

func (r *Replica) handleAccepted(m Accepted) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, r.ID())
	p := r.GetPaxos(m.Key, m.Table)

	needExec := p.HandleP2b(m)
	//log.Debugf("handleAccepted needExec %t\n", needExec)

	if needExec {
		p.GetAccessToken()
		defer p.ReleaseAccessToken()
		p.Exec()
	}

}

func (r *Replica) handleCommit(m Commit) {
	log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, r.ID())
	r.init(m.Command.Key, m.Command.Table)
	p := r.GetPaxosByCmd(m.Command)

	p.HandleP3(m)

}

func (r *Replica) handleExec(m Exec) {
	log.Debugf("Replica ===[%v]===>>> Replica %s\n", m, r.ID())
	r.init(m.Command.Key, m.Command.Table)
	p := r.GetPaxosByCmd(m.Command)

	p.GetAccessToken()
	p.epochSlot = m.EpochSlot
	p.execute = utils.Max(p.execute, m.EpochSlot)
	p.ReleaseAccessToken()

	m2 := Commit{
		Slot:m.Slot,
		Command:m.Command,
	}

	p.HandleP3(m2)

}


func (r *Replica) CountKeys() int {
	sum := 0
	r.RLock()
	defer r.RUnlock()

	for _, table := range r.tables {
		sum += table.CountKeys()
	}

	return sum
}

