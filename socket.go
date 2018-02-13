package fleetdb

import "github.com/acharapko/fleetdb/log"

type Socket interface {

	// Send put msg to outbound queue
	Send(to ID, msg interface{})

	// Multicast send msg to all nodes in the same site
	Multicast(zone int, msg interface{})

	// Broadcast send to all peers within the Replication Region
	//zone is the current zone of the node
	RBroadcast(zone int, msg interface{})

	// Broadcast send to all peers
	Broadcast(msg interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	GetReplicationGroupZones(zone int) []int
}

type socket struct {
	id    ID
	nodes map[ID]Transport
	codec Codec
}

func NewSocket(id ID, addrs map[ID]string, transport, codec string) Socket {
	socket := new(socket)
	socket.id = id
	socket.nodes = make(map[ID]Transport)
	socket.codec = NewCodec(codec)

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
	return socket
}

func (sock *socket) GetReplicationGroupZones(zone int) []int {
	z := make([]int, 1)
	z[0] = zone
	return z
}

func (sock *socket) Send(to ID, msg interface{}) {
	hlcTime := HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	sock.SendWithHeader(to, msg, hdr)
}

func (sock *socket) SendWithHeader(to ID, msg interface{}, header MsgHeader) {
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
	HLClock.Update(hdr.HLCTime)
	return msg
}

func (sock *socket) Multicast(zone int, msg interface{}) {
	hlcTime := HLClock.Now()
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

func (sock *socket) RBroadcast(zone int, msg interface{}) {
	hlcTime := HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	zones := sock.GetReplicationGroupZones(zone)
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
	hlcTime := HLClock.Now()
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
