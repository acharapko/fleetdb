package netwrk

import (
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/utils/hlc"
)

type Socket interface {

	// Send put msg to outbound queue
	Send(to ids.ID, msg interface{})

	// Multicast send msg to all nodes in the same site
	Multicast(zone uint8, msg interface{})

	// Broadcast send to all peers within the Replication Region
	//zone is the current zone of the node
	RBroadcast(zone uint8, msg interface{})

	// Broadcast send to all peers
	Broadcast(msg interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	GetReplicationGroupZones(zone uint8) []uint8
}

type socket struct {
	id    ids.ID
	nodes map[ids.ID]Transport
	codec Codec
}

func NewSocket(id ids.ID, addrs map[ids.ID]string, transport, codec string) Socket {
	log.Infof("Starting network layer at node %v \n", ids.GetID())
	socket := new(socket)
	socket.id = id
	socket.nodes = make(map[ids.ID]Transport)
	socket.codec = NewCodec(codec)

	log.Infof("Starting listening at %v \n", addrs[id])
	socket.nodes[id] = NewTransport(transport + "://" + addrs[id])
	go socket.nodes[id].Listen()

	for id, addr := range addrs {
		if id == socket.id {
			continue
		}
		t := NewTransport(transport + "://" + addr)
		err := t.Dial()
		for err != nil {
			err = t.Dial()
		}
		socket.nodes[id] = t
	}
	log.Infof("Started network layer at node %v \n", ids.GetID())
	return socket
}

func (sock *socket) GetReplicationGroupZones(zone uint8) []uint8 {
	z := make([]uint8, 1)
	z[0] = zone
	return z
}

func (sock *socket) Send(to ids.ID, msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	sock.SendWithHeader(to, msg, hdr)
}

func (sock *socket) SendWithHeader(to ids.ID, msg interface{}, header MsgHeader) {
	t, ok := sock.nodes[to]
	if !ok {
		log.Fatalf("transport of ID %v does not exists", to)
	}
	b := sock.codec.Encode(msg)
	h := header.ToBytes()
	m := NewMessage(len(b))
	m.Header = h
	m.Body = b
	t.Send(m)
}

func (sock *socket) Recv() interface{} {
	m := sock.nodes[sock.id].Recv()
	msg := sock.codec.Decode(m.Body)
	hdr := NewMsgHeaderFromBytes(m.Header)
	//log.Debugf("RECVD MSG HEADER : %v\n", hdr)
	hlc.HLClock.Update(hdr.HLCTime)
	return msg
}

func (sock *socket) Multicast(zone uint8, msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	for id := range sock.nodes {
		if id == sock.id {
			continue
		}
		if id.Zone() == zone {
			sock.SendWithHeader(id, msg, hdr)
		}
	}
}

func (sock *socket) RBroadcast(zone uint8, msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	zones := sock.GetReplicationGroupZones(zone)
	log.Debugf("RBroadcast to zones: %v\n", zones)
	for _, z := range zones {
		for id := range sock.nodes {
			if id == sock.id {
				continue
			}
			if id.Zone() == z {
				sock.SendWithHeader(id, msg, hdr)
			}
		}
	}
}

func (sock *socket) Broadcast(msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	for id := range sock.nodes {
		if id == sock.id {
			continue
		}
		sock.SendWithHeader(id, msg, hdr)
	}
}

func (sock *socket) Close() {
	for _, t := range sock.nodes {
		t.Close()
	}
}
