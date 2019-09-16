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
	"github.com/acharapko/fleetdb/kv_store"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
)

// Client main access point of bench lib
type Client struct {
	ID        ids.ID // client id use the same id as servers in local site
	N         int
	addrs     map[ids.ID]string
	http      map[ids.ID]string

	cid   ids.CommandID
	txNum int

	tempCmds []kv_store.Command
	results map[ids.CommandID]bool

	sync.RWMutex
	sync.WaitGroup
}

// NewClient creates a new Client from config
func NewClient() *Client {
	config := config.Instance
	ids.GetID()
	fmt.Printf("Starting Client %v\n", ids.GetID())
	c := new(Client)
	c.ID = ids.GetID()
	c.N = len(config.Addrs)
	c.addrs = config.Addrs
	c.http = config.HTTPAddrs
	c.txNum = 0
	c.results = make(map[ids.CommandID]bool, config.BufferSize)
	return c
}

func (c *Client) getNodeID(key kv_store.Key) ids.ID {
	//TODO: select random node in the zone
	id := ids.NewID(c.ID.Zone(), 1)

	return id
}

// RESTGet access server's REST API with url = http://ip:port/key
func (c *Client) RESTGet(key kv_store.Key, table string) kv_store.Value {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + table + "/" + string(key)

	log.Debugf("RESTGET %s cid=%d, url=%s\n", string(key), c.cid, url)

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
		log.Debugf("type=%s key=%v value=%x", "get", string(key), kv_store.Value(b))
		return kv_store.Value(b)
	}
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugf("%q", dump)
	return nil
}

// RESTDelete access server's REST API with url = http://ip:port/key
func (c *Client) RESTDelete(key kv_store.Key, table string) bool {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + table + "/" + string(key)

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
func (c *Client) RESTPut(key kv_store.Key, value kv_store.Value, table string) {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + table + "/" + string(key)
	log.Debugf("RESTPUT %s cid=%d, url=%s\n", string(key), c.cid, url)
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
func (c *Client) Get(key kv_store.Key, table string) kv_store.Value {
	return c.RESTGet(key, table)
}

// Put post json request
func (c *Client) Put(key kv_store.Key, value kv_store.Value, table string) {
	c.RESTPut(key, value, table)
}

// Delete post json request
func (c *Client) Delete(key kv_store.Key, table string) {
	c.RESTPut(key, nil, table)
}

// GetAsync do Get request in goroutine
func (c *Client) GetAsync(key kv_store.Key, table string) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Get(key, table)
}

// PutAsync do Put request in goroutine
func (c *Client) PutAsync(key kv_store.Key, value kv_store.Value, table string) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Put(key, value, table)
}

// Put post json request
func (c *Client) PutTx(keys []kv_store.Key, values []kv_store.Value, tables []string) bool {
	c.txNum++
	cmds := make([]kv_store.Command, len(keys))
	cntr := 0
	for i, k := range keys {
		c.cid++
		cmd := kv_store.Command{tables[i], k, values[i],c.ID,c.cid, kv_store.PUT }
		cmds[cntr] = cmd
		cntr++
	}
	return c.JSONTX(cmds)
}

func (c *Client) PrepTx() {
	c.txNum++
	c.tempCmds = make([]kv_store.Command, 0)
}

// Add put for future TX
func (c *Client) AddTxPut(key kv_store.Key, value kv_store.Value, table string) {
	c.cid++
	cmd := kv_store.Command{table,key, value,c.ID,c.cid, kv_store.PUT }
	c.tempCmds = append(c.tempCmds, cmd)
}

func (c *Client) AddTxDelete(key kv_store.Key, table string) {
	c.cid++
	cmd := kv_store.Command{table,key, nil,c.ID,c.cid, kv_store.DELETE }
	c.tempCmds = append(c.tempCmds, cmd)
}

// Add get for future TX
func (c *Client) AddTxGet(key kv_store.Key, table string) {
	c.cid++
	cmd := kv_store.Command{table,key, nil,c.ID,c.cid, kv_store.GET }

	c.tempCmds = append(c.tempCmds, cmd)
}

func (c *Client) SendTX() bool {
	return c.JSONTX(c.tempCmds)
}

func (c *Client) JSONGet(key kv_store.Key, table string) kv_store.Value {
	c.cid++
	cmd := kv_store.Command{ table,key, nil, c.ID, c.cid, kv_store.GET}
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
		return kv_store.Value(b)
	}
	return nil
}

func (c *Client) JSONPut(key kv_store.Key, value kv_store.Value, table string) {
	c.cid++
	cmd := kv_store.Command{table, key, value, c.ID,c.cid, kv_store.PUT}
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

func (c *Client) JSONTX(commands []kv_store.Command) bool {
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
	if rep != nil {
		log.Debugf("TX ok=%s", rep.Header["Ok"][0] )
		defer rep.Body.Close()
		if rep.Header["Ok"][0] == "true" {
			return true
		}
	}
	if err != nil {
		log.Errorln(err)
		return false
	}
	log.Debugln(rep.Status)
	return false
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
