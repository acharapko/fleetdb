package main

import (
	"flag"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/key_value"
	"strconv"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
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

func (d *db) Read(k int) key_value.Value {
	key := []byte(strconv.Itoa(k))
	v := d.c.Get(key_value.Key(key), "test")
	return v
}

func (d *db) Write(k int, v []byte) {
	key := []byte(strconv.Itoa(k))
	d.c.Put(key_value.Key(key), key_value.Value(v), "test")
}

func (d *db) WriteStr(k int, v string) {
	d.Write(k, []byte(v))
}

func (d *db) TxWrite(ks []int, v []key_value.Value) bool {
	bkeys := make([]key_value.Key, len(ks))
	vals := make([]key_value.Value, len(ks))
	tbls := make([]string, len(ks))
	for i, k := range ks {
		key := []byte(strconv.Itoa(k))
		vals[i] = v[i]
		bkeys[i] = key_value.Key(key)
		tbls[i] = "test"
	}

	return d.c.PutTx(bkeys, vals, tbls)

}

func main() {
	flag.Parse()

	id := ids.GetID()

	var cfg config.Config
	if *master == "" {
		cfg = config.NewConfig(id)
		log.Infof("Starting Benchmark %s \n", id)
	} else {
		cfg = config.ConnectToMaster(*master, true, id)
		log.Infof("Received config %s\n", cfg)
	}

	d := new(db)
	d.c = fleetdb.NewClient(cfg)

	b := fleetdb.NewBenchmarker(d)
	b.Load()
	log.Infof("Benchmarker runs with concurrency = %d", b.Concurrency)
	b.Run()
}
