package main

import (
	"flag"

	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/db_node"

	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
)

var master = flag.String("master", "", "Master address.")

var simulation = flag.Bool("simulation", false, "Mocking network by chan and goroutine.")
var n = flag.Int("n", 3, "number of servers in each zone")
var memprof = flag.String("memprof", "", "write memory profile to this file")

var m = flag.Int("m", 3, "number of zones")

func replica(id ids.ID) {
	var cfg config.Config
	if *master == "" {
		cfg = config.NewConfig(id)
		log.Info("No Master config")
	} else {
		cfg = config.ConnectToMaster(*master, false, id)
		log.Info("Master config")
	}

	if *simulation {
		cfg.Transport = "chan"
	}

	log.Infof("server %v starting\n", cfg.ID)

	dbInstance := db_node.NewDBNode(cfg)
	if memprof != nil {
		dbInstance.SetMemProfile(*memprof)
	}
	dbInstance.Run();

	log.Infof("server %v started\n", cfg.ID)
}

func main() {
	flag.Parse()
	id := ids.GetID()
	replica(id)
}
