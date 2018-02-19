package fleetdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/utils/hlc"
	"time"
	"sync/atomic"
)

var (
	// NumZones total number of sites
	NumZones int
	// NumNodes total number of nodes
	NumNodes int
	// NumLocalNodes number of nodes per site
	NumLocalNodes int
	// F number of zone failures
	F int
	// QuorumType name of the quorums
	QuorumType string

	//Used for Object Hnadowver decisions
	HandoverN int
	HandoverInterval int

	//HLC
	HLClock *hlc.HLC
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	Socket
	Store
	ID() ID
	Config() Config
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
	GetTX(txid TXID) *Transaction
	//ExecTx(txid TXID, key Key, tx *Transaction) bool
}

// node implements Node interface
type node struct {
	id     ID
	config Config

	txCount int32

	Socket
	Store
	MessageChan chan interface{}
	handles     map[string]reflect.Value
}

// NewNode creates a new Node object from configuration
func NewNode(config Config) Node {
	node := new(node)
	node.id = config.ID
	node.config = config

	node.Socket = NewSocket(config.ID, config.Addrs, config.Transport, config.Codec)
	node.Store = NewStore(config)
	node.MessageChan = make(chan interface{}, config.ChanBufferSize)
	node.handles = make(map[string]reflect.Value)

	zones := make(map[int]int)
	for id := range config.Addrs {
		zones[id.Zone()]++
	}

	NumZones = len(zones)
	NumNodes = len(config.Addrs)
	HandoverN = config.handoverN
	HandoverInterval = config.Interval
	NumLocalNodes = zones[config.ID.Zone()]
	F = config.F
	QuorumType = config.Quorum
	HLClock = hlc.NewHLC(time.Now().Unix())

	return node
}


/*func (n *node) ExecTx(txid TXID, key Key, tx *Transaction) bool {
	//do nothing
	return false
}*/

func (n *node) GetTX(txid TXID) *Transaction {
	//do nothing
	return nil
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Config() Config {
	return n.config
}

func (n *node) Retry(r Request) {
	n.MessageChan <- r
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running\n", n.id)
	if len(n.handles) > 0 {
		go n.handle()
	}
	go n.recv()
	n.serve()
}

func (n *node) serveRequest(r *http.Request, w http.ResponseWriter) {

	var req Request
	req.C = make(chan Reply)
	clientID := ID(r.Header.Get("id"))
	cid, _ := strconv.Atoi(r.Header.Get("cid"))
	commandID := CommandID(cid)
	req.Timestamp, _ = strconv.ParseInt(r.Header.Get("timestamp"), 10, 64)

	key := []byte((r.URL.Path[1:]))
	switch r.Method {
	case http.MethodGet:
		req.Command = Command{key, nil, clientID, commandID, GET}
	case http.MethodPut, http.MethodPost:
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorln("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		req.Command = Command{key, Value(body), clientID, commandID, PUT}
	case http.MethodDelete:
		req.Command = Command{key, nil, clientID, commandID, DELETE}
	}

	n.MessageChan <- req

	reply := <-req.C

	if reply.Err != nil {
		if r.Method == http.MethodGet && reply.Err == ErrNotFound {
			http.Error(w, ErrNotFound.Error(), http.StatusNotFound)
		} else {
			http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// r.w.Header().Set("ok", fmt.Sprintf("%v", reply.OK))
	w.Header().Set("id", fmt.Sprintf("%v", reply.Command.ClientID))
	w.Header().Set("cid", fmt.Sprintf("%v", reply.Command.CommandID))
	w.Header().Set("timestamp", fmt.Sprintf("%v", reply.Timestamp))
	if reply.Command.IsRead() {
		_, err := io.WriteString(w, string(reply.Value))
		// _, err := r.w.Write(reply.Command.Value)
		if err != nil {
			log.Errorln(err)
		}
	}
}

func (n *node) serveTransaction(r *http.Request, w http.ResponseWriter) {
	var tx Transaction
	tx.c = make(chan TransactionReply)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("error reading body: ", err)
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	json.Unmarshal(body, &tx)
	tx.Timestamp = HLClock.Now().ToInt64()
	txc := atomic.AddInt32(&n.txCount, 1)
	tx.TxID = NewTXID(n.ID().Zone(), n.ID().Node(), int(txc))
	log.Debugf("Adding Tx to Message Chan TX %v {body=%v}\n", tx, body)
	n.MessageChan <- tx

	reply := <-tx.c
	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}
	// r.w.Header().Set("ok", fmt.Sprintf("%v", reply.OK))
	w.Header().Set("id", fmt.Sprintf("%v", reply.ClientID))
	w.Header().Set("cid", fmt.Sprintf("%v", reply.CommandID))
	w.Header().Set("timestamp", fmt.Sprintf("%v", reply.Timestamp))
	/*if reply.Command.IsRead() {
		_, err := io.WriteString(w, string(reply.Command.Value))
		// _, err := r.w.Write(reply.Command.Value)
		if err != nil {
			log.Errorln(err)
		}
	}*/
}

// serve serves the http REST API request from clients
func (n *node) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		if len(r.URL.Path) > 1 {
			n.serveRequest(r, w)
		} else {
			//we use JSON Body for TX only

			log.Debugf(" About to serve TX \n")

			n.serveTransaction(r, w)
		}
	})
	// http string should be in form of ":8080"
	url, _ := url.Parse(n.config.HTTPAddrs[n.id])
	port := ":" + url.Port()
	err := http.ListenAndServe(port, mux)
	if err != nil {
		log.Fatalln(err)
	}
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		n.MessageChan <- n.Recv()
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v})
	}
}



func (n *node) ForwardTx(id ID, tx Transaction) {
	//key := m.Command.Key
	//url := n.config.HTTPAddrs[id] + "/" + string(key)
}

func (n *node) Forward(id ID, m Request) {
	key := m.Command.Key
	url := n.config.HTTPAddrs[id] + "/" + string(key)

	log.Debugf("Node %v forwarding request %v to %s", n.ID(), m, url)
	switch m.Command.Operation {
	case GET:
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		req.Header.Set("id", fmt.Sprintf("%v", m.Command.ClientID))
		req.Header.Set("timestamp", fmt.Sprintf("%d", m.Timestamp))
		rep, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		defer rep.Body.Close()
		if rep.StatusCode == http.StatusOK {
			b, _ := ioutil.ReadAll(rep.Body)
			cmd := m.Command
			cmd.Value = Value(b)
			m.Reply(Reply{
				Command:   cmd,
				Value: Value(b),
			})
		}
	case PUT:
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(m.Command.Value))
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		req.Header.Set("id", fmt.Sprintf("%v", m.Command.ClientID))
		req.Header.Set("timestamp", fmt.Sprintf("%d", m.Timestamp))
		rep, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		defer rep.Body.Close()
		if rep.StatusCode == http.StatusOK {
			m.Reply(Reply{
				Command:m.Command,
			})
		}
	case DELETE:
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		req.Header.Set("id", fmt.Sprintf("%v", m.Command.ClientID))
		req.Header.Set("timestamp", fmt.Sprintf("%d", m.Timestamp))
		rep, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Errorln(err)
			m.Reply(Reply{
				Command: m.Command,
				Err:     err,
			})
			return
		}
		defer rep.Body.Close()
		if rep.StatusCode == http.StatusOK {
			m.Reply(Reply{
				Command:m.Command,
			})
		}
	}
}
