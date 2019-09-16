package main

import (
	"flag"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/kv_store"
	"strconv"
	"github.com/acharapko/fleetdb/ids"
)

var master = flag.String("master", "", "Master address.")

// db implements db.DB interface for benchmarking
type db struct {
	c *fleetdb.Client
}

func (d *db) Init() {
	d.c.Start()
}

func (d *db) Stop() {
	d.c.Stop()
}

func (d *db) Read(k int) kv_store.Value {
	key := []byte(strconv.Itoa(k))
	v := d.c.Get(kv_store.Key(key), "test")
	return v
}

func (d *db) Write(k int, v []byte) {
	key := []byte(strconv.Itoa(k))
	d.c.Put(kv_store.Key(key), kv_store.Value(v), "test")
}

func (d *db) WriteStr(k int, v string) {
	d.Write(k, []byte(v))
}

func (d *db) TxWrite(ks []int, v []kv_store.Value) bool {
	bkeys := make([]kv_store.Key, len(ks))
	vals := make([]kv_store.Value, len(ks))
	tbls := make([]string, len(ks))
	for i, k := range ks {
		key := []byte(strconv.Itoa(k))
		vals[i] = v[i]
		bkeys[i] = kv_store.Key(key)
		tbls[i] = "test"
	}

	return d.c.PutTx(bkeys, vals, tbls)
}

func main() {
	flag.Parse()
	id := ids.GetID()
	log.Infof("Starting Benchmark %s \n", id)
	d := new(db)
	d.c = fleetdb.NewClient()
	b := fleetdb.NewBenchmarker(d)
	b.Load()
	log.Infof("Benchmarker runs with concurrency = %d", b.Concurrency)
	b.Run()
}
