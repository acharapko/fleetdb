package main

import (
	"flag"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"strconv"
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

func (d *db) Read(k int) fleetdb.Value {
	key := []byte(strconv.Itoa(k))
	v := d.c.Get(fleetdb.Key(key))
	return v
}

func (d *db) Write(k int, v []byte) {
	key := []byte(strconv.Itoa(k))
	d.c.Put(fleetdb.Key(key), fleetdb.Value(v))
}

func (d *db) WriteStr(k int, v string) {
	d.Write(k, []byte(v))
}

func (d *db) TxWrite(ks []int, v []fleetdb.Value) {
	bkeys := make([]fleetdb.Key, len(ks))
	vals := make([]fleetdb.Value, len(ks))

	for i, k := range ks {
		key := []byte(strconv.Itoa(k))
		vals[i] = v[i]
		bkeys[i] = fleetdb.Key(key)
	}

	d.c.PutTx(bkeys, vals)

}

func main() {
	flag.Parse()

	id := fleetdb.GetID()

	var config fleetdb.Config
	if *master == "" {
		config = fleetdb.NewConfig(id)
		log.Infof("Starting Benchmark %s \n", id)
	} else {
		config = fleetdb.ConnectToMaster(*master, true, id)
		log.Infof("Received config %s\n", config)
	}

	d := new(db)
	d.c = fleetdb.NewClient(config)

	b := fleetdb.NewBenchmarker(d)
	b.Load()
	log.Infof("Benchmarker runs with concurrency = %d", b.Concurrency)
	b.Run()
}
