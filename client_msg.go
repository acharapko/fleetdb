package fleetdb

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/fleetdb/log"
)

func init() {
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(Read{})
	gob.Register(ReadReply{})
	gob.Register(TransactionReply{})
	gob.Register(Transaction{})
	gob.Register(Register{})
	gob.Register(Config{})
}

/***************************
 * Client-Replica Messages *
 ***************************/

// CommandID identifies commands from each bench, can be any integer type.
type CommandID uint64

// Request is bench reqeust with http response channel
type Request struct {
	Command   Command
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
	Command   Command
	Value 	  Value
	Timestamp int64
	Err       error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v}", r.Command)
}

// Read can be used as a special request that directly read the value of
// key without go through replication protocol in Replica
type Read struct {
	CommandID CommandID
	Key       Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID CommandID
	Value     Value
}

func (r ReadReply) String() string {
	return fmt.Sprintf("ReadReply {cid=%d, val=%v}", r.CommandID, r.Value)
}

// Transaction contains arbitrary number of commands in one request
type Transaction struct {
	TxID TXID
	CommandID	CommandID
	Commands  	[]Command
	CmdMeta 	[]TxCommandMeta

	ClientID  ID

	p3started bool

	Timestamp int64
	c         chan TransactionReply
	execChan  chan TxExec
}

func NewInProgressTX(TxID TXID, cmds []Command, s []int) *Transaction {

	cmdMetas := make([]TxCommandMeta, len(cmds))

	for i, slot := range s {
		cmdMeta := TxCommandMeta{false, false, slot}
		cmdMetas[i] = cmdMeta
	}

	inprogress := Transaction{
		TxID: TxID,
		Commands: cmds,
		CmdMeta:cmdMetas,
	}
	return &inprogress
}

func (t *Transaction) MakeCommittedWaitingFlags(slots []int) {
	cmdMetas := make([]TxCommandMeta, len(slots))

	for i := 0; i < len(slots); i++ {
		cmdMeta := TxCommandMeta{false, false, slots[i]}
		cmdMetas[i] = cmdMeta
	}
	t.CmdMeta = cmdMetas
}

func (t Transaction) String() string {
	return fmt.Sprintf("Transaction {client id=%s, tx id=%v, cmds=%v}", t.ClientID, t.TxID, t.Commands)
}


// Reply replies to the current request
func (r *Transaction) Reply(reply TransactionReply) {
	r.c <- reply
}

// Reply replies to the current request
func (r *Transaction) ReadyToExec(slot int, key Key) {
	r.execChan <- TxExec{Slotnum:slot, Key:key}
}

func (r *Transaction) MakeExecChannel() {
	r.execChan = make(chan TxExec)
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
		if !meta.cmdCommitted {
			return false
		}
	}
	return true
}

func (tx *Transaction) MarkCommitted(key Key) {
	log.Debugf("Marking Committed: len(meta) = %d\n", len(tx.CmdMeta))
	for i, cmd := range tx.Commands {
		if cmd.Key.B64() == key.B64() && !tx.CmdMeta[i].cmdCommitted {
			tx.CmdMeta[i].cmdCommitted = true
			return
		}
	}
}

func (tx *Transaction) AreAllWaiting() bool  {
	for _, meta := range tx.CmdMeta {
		if !meta.cmdWaitingExec{
			return false
		}
	}
	return true
}

func (tx *Transaction) MarkWaiting(key Key) {
	log.Debugf("Marking Waiting key %v in TX %v\n", string(key), tx.TxID)
	for i, cmd := range tx.Commands {
		if cmd.Key.B64() == key.B64() && !tx.CmdMeta[i].cmdWaitingExec {
			tx.CmdMeta[i].cmdWaitingExec = true
			return
		}
	}
}

// TransactionReply is the result of transaction struct
type TransactionReply struct {
	OK        bool
	CommandID CommandID
	LeaderID  ID
	ClientID  ID
	Commands  []Command
	Timestamp int64
	Err       error
}

type TxExec struct {
	Slotnum		int
	Key			Key
}

func (r TxExec) String() string {
	return fmt.Sprintf("TxExec {key=%v, slotnum=%d}", r.Key, r.Slotnum)
}

/**************************
 *     Config Related     *
 **************************/

// Register message type is used to regitster self (node or bench) with master node
type Register struct {
	Client bool
	ID     ID
	Addr   string
}
