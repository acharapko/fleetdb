package fleetdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"time"

	"github.com/acharapko/fleetdb/log"
)

// Client main access point of bench lib
type Client struct {
	ID        ID // client id use the same id as servers in local site
	N         int
	addrs     map[ID]string
	http      map[ID]string

	cid   CommandID
	txNum int

	tempCmds []Command
	results map[CommandID]bool

	sync.RWMutex
	sync.WaitGroup
}

// NewClient creates a new Client from config
func NewClient(config Config) *Client {
	fmt.Printf("Starting Client %v\n", config.ID)
	c := new(Client)
	c.ID = config.ID
	c.N = len(config.Addrs)
	c.addrs = config.Addrs
	c.http = config.HTTPAddrs
	c.txNum = 0
	c.results = make(map[CommandID]bool, config.BufferSize)
	return c
}

func (c *Client) getNodeID(key Key) ID {
	//TODO: select random node in the zone
	id := NewID(c.ID.Zone(), 1)

	return id
}

// RESTGet access server's REST API with url = http://ip:port/key
func (c *Client) RESTGet(key Key) Value {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + string(key)

	log.Debugf("RESTGET %s\n", string(key))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	req.Header.Set("id", fmt.Sprintf("%v", c.ID))
	req.Header.Set("cid", strconv.FormatUint(uint64(c.cid), 10))
	req.Header.Set("timestamp", strconv.FormatInt(time.Now().UnixNano(), 10))
	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(rep.Body)
		log.Debugf("type=%s key=%v value=%x", "get", string(key), Value(b))
		return Value(b)
	}
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return nil
}

// RESTDelete access server's REST API with url = http://ip:port/key
func (c *Client) RESTDelete(key Key) bool {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + string(key)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		log.Errorln(err)
		return false
	}
	req.Header.Set("id", fmt.Sprintf("%v", c.ID))
	req.Header.Set("cid", strconv.FormatUint(uint64(c.cid), 10))
	req.Header.Set("timestamp", strconv.FormatInt(time.Now().UnixNano(), 10))
	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorln(err)
		return false
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		log.Debugf("type=%s key=%v", "delete", key)
		return true
	}
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return false
}

// RESTPut access server's REST API with url = http://ip:port/key and request body of value
func (c *Client) RESTPut(key Key, value Value) {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + string(key)
	log.Debugf("RESTPUT %s\n", string(key))
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(value))
	if err != nil {
		log.Errorln(err)
		return
	}
	req.Header.Set("id", fmt.Sprintf("%v", c.ID))
	req.Header.Set("cid", fmt.Sprintf("%v", c.cid))
	req.Header.Set("timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))
	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorln(err)
		return
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		log.Debugf("type=%s key=%v value=%x", "put", string(key), value)
	} else {
		dump, _ := httputil.DumpResponse(rep, true)
		log.Debugf("%q", dump)
	}
}

// Get post json get request to server url
func (c *Client) Get(key Key) Value {
	return c.RESTGet(key)
}

// Put post json request
func (c *Client) Put(key Key, value Value) {
	c.RESTPut(key, value)
}

// Put post json request
func (c *Client) PutTx(keys []Key, values []Value) {
	c.txNum++
	cmds := make([]Command, len(keys))
	cntr := 0
	for i, k := range keys {
		c.cid++
		cmd := Command{k, values[i],c.ID,c.cid, PUT }
		cmds[cntr] = cmd
		cntr++
	}
	c.JSONTX(cmds)
}

func (c *Client) PrepTx() {
	c.txNum++
	c.tempCmds = make([]Command, 0)
}

// Add put for future TX
func (c *Client) AddTxPut(key Key, value Value) {
	c.cid++
	cmd := Command{key, value,c.ID,c.cid, PUT }

	c.tempCmds = append(c.tempCmds, cmd)
}

// Add get for future TX
func (c *Client) AddTxGet(key Key) {
	c.cid++
	cmd := Command{key, nil,c.ID,c.cid, GET }

	c.tempCmds = append(c.tempCmds, cmd)
}

func (c *Client) SendTX() {
	c.JSONTX(c.tempCmds)
}

// Delete post json request
func (c *Client) Delete(key Key) {
	c.RESTPut(key, nil)
}

// GetAsync do Get request in goroutine
func (c *Client) GetAsync(key Key) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Get(key)
}

// PutAsync do Put request in goroutine
func (c *Client) PutAsync(key Key, value Value) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Put(key, value)
}

func (c *Client) JSONGet(key Key) Value {
	c.cid++
	cmd := Command{ key, nil, c.ID, c.cid, GET}
	req := new(Request)
	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()

	id := c.getNodeID(key)

	url := c.http[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(rep.Body)
		return Value(b)
	}
	return nil
}

func (c *Client) JSONPut(key Key, value Value) {
	c.cid++
	cmd := Command{key, value, c.ID,c.cid, PUT}
	req := new(Request)
	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()

	id := c.getNodeID(key)

	url := c.http[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Errorln(err)
		return
	}
	defer rep.Body.Close()
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugln(rep.Status)
	log.Debugf("%q", dump)
}

func (c *Client) JSONTX(commands []Command) {
	c.cid++
	tx := new(Transaction)
	tx.ClientID = c.ID
	tx.CommandID = c.cid
	for _, cmd := range commands {
		tx.Commands = append(tx.Commands, cmd)
	}

	url := c.http[c.getNodeID(commands[0].Key)]
	log.Debugf("TX: %v\n", tx)
	data, err := json.Marshal(*tx)
	rep, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	defer rep.Body.Close()
	if err != nil {
		log.Errorln(err)
		return
	}
	//dump, _ := httputil.DumpResponse(rep, true)
	log.Debugln(rep.Status)
	//log.Debugf("%q", dump)
}

// RequestDone returns the total number of succeed async reqeusts
func (c *Client) RequestDone() int {
	sum := 0
	for _, succeed := range c.results {
		if succeed {
			sum++
		}
	}
	return sum
}

func (c *Client) Start() {}

func (c *Client) Stop() {}
