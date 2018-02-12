package fleetdb

import (
	"fmt"
	"bytes"
)

type Operation uint8

const (
	NOOP Operation = iota
	PUT
	GET
	DELETE
	TX_LEASE
)

type Command struct {
	Key       Key
	Value     Value
	ClientID  ID
	CommandID CommandID
	Operation Operation
}

func (c Command) String() string {
	switch c.Operation {
	case GET:
		return fmt.Sprintf("Get{key=%v, id=%s, cid=%d}", string(c.Key), c.ClientID, c.CommandID)
	case PUT:
		return fmt.Sprintf("Put{key=%v, value=%v, id=%s, cid=%d}", string(c.Key), c.Value, c.ClientID, c.CommandID)
	case DELETE:
		return fmt.Sprintf("Delete{key=%v, id=%s, cid=%d}", string(c.Key), c.ClientID, c.CommandID)
	case TX_LEASE:
		return fmt.Sprintf("TXLease{key=%v, id=%s, cid=%d}", string(c.Key), c.ClientID, c.CommandID)
	case NOOP:
		return fmt.Sprintf("NOOP{key=%v, id=%s, cid=%d}", string(c.Key), c.ClientID, c.CommandID)
	}
	return fmt.Sprintf("Unknown{key=%v, id=%s, cid=%d}", string(c.Key), c.ClientID, c.CommandID)
}

// IsRead returns true if command is read
func (c Command) IsRead() bool {
	return c.Operation == GET
}

// Equal returns true if two commands are equal
func (c Command) Equal(a Command) bool {
	return c.Key.B64() == a.Key.B64() && bytes.Equal(c.Value, a.Value) && c.ClientID == a.ClientID && c.CommandID == a.CommandID
}

type TxCommandMeta struct {
	cmdCommitted bool
	cmdWaitingExec bool
	Slot int
}