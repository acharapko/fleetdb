package main

import (
	"flag"

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

	log.Infof("server %v started\n", config.ID)
}

func main() {
	flag.Parse()
	id := fleetdb.GetID()
	replica(id)
}
