package fleetdb

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/kv_store"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
)

func init() {
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(TransactionReply{})
	gob.Register(Transaction{})
	gob.Register(config.Config{})
}

/***************************
 * Client-Replica Messages *
 ***************************/

// Request is bench reqeust with http response channel
type Request struct {
	Command   kv_store.Command
	Timestamp int64

	C chan Reply
}

// Reply replies to the current request
func (r *Request) Reply(reply Reply) {
	r.C <- reply
}

func (r Request) String() string {
	return fmt.Sprintf("Request {cmd=%v}", r.Command)
}

// Reply includes all info that might replies to back the client for the corresponding request
type Reply struct {
	Command   kv_store.Command
	Value     kv_store.Value
	Timestamp int64
	Err       error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v}", r.Command)
}

// Read can be used as a special request that directly read the value of
// key without go through replication protocol in Replica
type Read struct {
	CommandID ids.CommandID
	Key       kv_store.Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID ids.CommandID
	Value     kv_store.Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%v}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
type Transaction struct {
	TxID ids.TXID
	CommandID	ids.CommandID
	Commands  	[]kv_store.Command
	CmdMeta 	[]kv_store.TxCommandMeta

	ClientID  ids.ID

	p3started bool

	Timestamp int64
	c         chan TransactionReply
	execChan  chan TxExec
}

func NewInProgressTX(TxID ids.TXID, cmds []kv_store.Command, s []int) *Transaction {

	cmdMetas := make([]kv_store.TxCommandMeta, len(cmds))

	for i, slot := range s {
		cmdMeta := kv_store.TxCommandMeta{false, false, slot}
		cmdMetas[i] = cmdMeta
	}

	inprogress := Transaction{
		TxID: TxID,
		Commands: cmds,
		CmdMeta:cmdMetas,
	}
	inprogress.MakeExecChannel(len(cmds))
	return &inprogress
}

func (t *Transaction) MakeCommittedWaitingFlags(slots []int) {
	cmdMetas := make([]kv_store.TxCommandMeta, len(slots))

	for i := 0; i < len(slots); i++ {
		cmdMeta := kv_store.TxCommandMeta{false, false, slots[i]}
		cmdMetas[i] = cmdMeta
	}
	t.CmdMeta = cmdMetas
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {client id=%s, tx id=%v, cmds=%v}", t.ClientID, t.TxID, t.Commands)
}


// Reply replies to the current request
func (r *Transaction) Reply(reply TransactionReply) {
	if r.c != nil {
		r.c <- reply
	}
}

// Reply replies to the current request
func (r *Transaction) ReadyToExec(slot int, key kv_store.Key) {
	if r.execChan != nil {
		r.execChan <- TxExec{Slotnum: slot, Key: key}
	}
}

func (r *Transaction) MakeExecChannel(numkeys int) {
	//we have a buffered channel in case we are sending to it before the consumer is created
	r.execChan = make(chan TxExec, numkeys)
}

func (r *Transaction) CloseExecChannel() {
	close(r.execChan)
}

func (r *Transaction) GetExecChannel() chan TxExec {
	return r.execChan
}

func (tx *Transaction) P3Sent()  {
	tx.p3started = true
}

func (tx *Transaction) CanSendP3() bool  {
	return !tx.p3started
}

func (tx *Transaction) AreAllCommitted() bool  {
	for _, meta := range tx.CmdMeta {
		if !meta.CmdCommitted {
			return false
		}
	}
	return true
}

func (tx *Transaction) MarkCommitted(key kv_store.Key) {
	log.Debugf("Marking Committed: len(meta) = %d\n", len(tx.CmdMeta))
	for i, cmd := range tx.Commands {
		if cmd.Key.B64() == key.B64() && !tx.CmdMeta[i].CmdCommitted {
			tx.CmdMeta[i].CmdCommitted = true
			return
		}
	}
}

func (tx *Transaction) AreAllWaiting() bool  {
	for _, meta := range tx.CmdMeta {
		if !meta.CmdWaitingExec{
			return false
		}
	}
	return true
}

func (tx *Transaction) MarkWaiting(key kv_store.Key) {
	log.Debugf("Marking Waiting key %v in TX %v\n", string(key), tx.TxID)
	for i, cmd := range tx.Commands {
		if cmd.Key.B64() == key.B64() && !tx.CmdMeta[i].CmdWaitingExec {
			tx.CmdMeta[i].CmdWaitingExec = true
			return
		}
	}
}

// TransactionReply is the result of transaction struct
type TransactionReply struct {
	OK        bool
	CommandID ids.CommandID
	LeaderID  ids.ID
	ClientID  ids.ID
	Commands  []kv_store.Command
	Timestamp int64
	Err       error
}

type TxExec struct {
	Slotnum int
	Key     kv_store.Key
}

func (r TxExec) String() string {
	return fmt.Sprintf("TxExec {key=%v, slotnum=%d}", r.Key, r.Slotnum)
}
