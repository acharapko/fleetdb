package netwrk

import (
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/utils/hlc"
)

type ClientSocket interface {

	// Send put msg to outbound queue
	Send(msg interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

}

type clientSocket struct {
	connectedID     ids.ID
	connectedNode	Transport
	codec 			Codec
}

func NewClientSocket(nodeId ids.ID, addrs map[ids.ID]string, transport, codec string) ClientSocket {
	socket := new(clientSocket)
	socket.connectedID = nodeId
	socket.codec = NewCodec(codec)

	for id, addr := range addrs {
		if id == nodeId {
			t := NewTransport(transport + "://" + addr)
			err := t.Dial()
			for err != nil {
				err = t.Dial()
			}
			socket.connectedNode = t
			continue
		}
	}
	return socket
}


func (sock *clientSocket) Send(msg interface{}) {
	hlcTime := hlc.HLClock.Now()
	hdr := MsgHeader{HLCTime:hlcTime}
	sock.SendWithHeader(sock.connectedID, msg, hdr)
}

func (sock *clientSocket) SendWithHeader(to ids.ID, msg interface{}, header MsgHeader) {

	b := sock.codec.Encode(msg)
	h := header.ToBytes()
	m := NewMessage(len(b))
	m.Header = h
	m.Body = b
	sock.connectedNode.Send(m)
}

func (sock *clientSocket) Recv() interface{} {
	m := sock.connectedNode.Recv()
	msg := sock.codec.Decode(m.Body)
	hdr := NewMsgHeaderFromBytes(m.Header)
	//log.Debugf("RECVD MSG HEADER : %v\n", hdr)
	hlc.HLClock.Update(hdr.HLCTime)
	return msg
}

func (sock *clientSocket) Close() {
	sock.connectedNode.Close()
}
