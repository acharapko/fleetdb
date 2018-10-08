package main

import (
	"flag"

	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/db_node"

	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
)

var master = flag.String("master", "", "Master address.")
var n = flag.Int("n", 3, "number of servers in each zone")
var m = flag.Int("m", 3, "number of zones")

var memprof = flag.String("memprof", "", "write memory profile to this file")

func replica() {
	log.Infof("Server %v starting\n", ids.GetID())
	config.LoadConfig() // load config upon startup of a replica
	log.Infof("leveldb: %v \n", config.GetConfig().LevelDBDir)
	log.Infof("quorum: %v \n", config.GetConfig().Quorum)
	dbInstance := db_node.NewDBNode() // start a DB Node
	if memprof != nil {
		dbInstance.SetMemProfile(*memprof)
	}

	dbInstance.Run(); // run the DB node
	log.Infof("server %v started\n", ids.GetID())
}

func main() {
	flag.Parse()
	replica()
}
