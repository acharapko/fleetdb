package config

import (
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/log"
	"net"
	"strconv"
	"encoding/gob"
)

func init() {
	gob.Register(Register{})
}

// Register message type is used to regitster self (node or bench) with master node
type Register struct {
	Client bool
	ID     ids.ID
	Addr   string
}


func ConnectToMaster(addr string, client bool, id ids.ID) Config {
	conn, err := net.Dial("tcp", addr+":"+strconv.Itoa(PORT))
	if err != nil {
		log.Fatalln(err)
	}
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	msg := &Register{
		Client: client,
		ID:     id,
		Addr:   "",
	}
	enc.Encode(msg)
	var config Config
	err = dec.Decode(&config)
	if err != nil {
		log.Fatalln(err)
	}
	return config
}
