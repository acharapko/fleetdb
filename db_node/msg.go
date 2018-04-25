package db_node

import (
	"encoding/gob"
	"fmt"
	"github.com/acharapko/fleetdb/ids"
)

func init() {
	gob.Register(GossipBalance{})
	gob.Register(ProximityPingRequest{})
	gob.Register(ProximityPingResponse{})
}

// Load Gossip
type GossipBalance struct {
	Items int
	From   ids.ID
}

func (gb GossipBalance) String() string {
	return fmt.Sprintf("GossipBalance {balance=%d items @ node %s}", gb.Items, gb.From)
}

//proximity ping

type ProximityPingRequest struct {
	From ids.ID
	TimeSent int64
}

func (ppr ProximityPingRequest) String() string {
	return fmt.Sprintf("ProximityPingRequest {node %s, SentTime=%d}", ppr.From, ppr.TimeSent)
}

type ProximityPingResponse struct {
	From ids.ID
	TimeSent int64
}

func (ppr ProximityPingResponse) String() string {
	return fmt.Sprintf("ProximityPingResponse {node %s, SentTime=%d}", ppr.From, ppr.TimeSent)
}