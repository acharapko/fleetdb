package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync"

	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/db"

)

var master = flag.String("master", "", "Master address.")

var simulation = flag.Bool("simulation", false, "Mocking network by chan and goroutine.")
var n = flag.Int("n", 3, "number of servers in each zone")
var memprof = flag.String("memprof", "", "write memory profile to this file")

var m = flag.Int("m", 3, "number of zones")

func replica(id fleetdb.ID) {
	var config fleetdb.Config
	if *master == "" {
		config = fleetdb.NewConfig(id)
		log.Info("No Master config")
	} else {
		config = fleetdb.ConnectToMaster(*master, false, id)
		log.Info("Master config")
	}

	if *simulation {
		config.Transport = "chan"
	}

	log.Infof("server %v starting\n", config.ID)

	dbInstance := db.NewDBNode(config)
	if memprof != nil {
		dbInstance.SetMemProfile(*memprof)
	}
	dbInstance.Run();

	//replica := wpaxos2.NewReplica(config)
	//replica.Run()
	log.Infof("server %v started\n", config.ID)

}

// not used
func mockConfig() fleetdb.Config {
	addrs := make(map[fleetdb.ID]string, *m**n)
	http := make(map[fleetdb.ID]string, *m**n)
	p := 0
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := fleetdb.ID(fmt.Sprintf("%d.%d", i, j))
			addrs[id] = "127.0.0.1:" + strconv.Itoa(fleetdb.PORT+p)
			http[id] = "http://127.0.0.1:" + strconv.Itoa(fleetdb.HTTP_PORT+p)
			p++
		}
	}

	c := fleetdb.MakeDefaultConfig()
	c.Addrs = addrs
	c.HTTPAddrs = http
	return c
}

func mockNodes() {
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := fleetdb.ID(fmt.Sprintf("%d.%d", i, j))
			go replica(id)
		}
	}
}

func main() {
	flag.Parse()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		mockNodes()
		wg.Wait()
	}

	id := fleetdb.GetID()
	replica(id)
}
