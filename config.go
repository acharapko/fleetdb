package fleetdb

import (
	"encoding/json"
	"flag"
	"os"
	"strconv"

	"github.com/acharapko/fleetdb/log"
)

var config = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")

// default values
const (
	PORT      = 1735
	HTTP_PORT = 8080
	BUFFER_SIZE      = 1024 * 1
	// TODO merge below two value with with config
	CHAN_BUFFER_SIZE = 1024 * 1
)

type Config struct {
	ID              ID            `json:"-"`
	Addrs           map[ID]string `json:"address"`      // address for node communication
	HTTPAddrs       map[ID]string `json:"http_address"` // address for bench server communication
	Quorum          string        `json:"quorum"`       // type of the quorums
	F               int           `json:"f"`            // number of failure zones in general grid quorums
	RS              int           `json:"rs"`            // number of failure zones in a replication region
	LevelDBDir      []string      `json:"leveldb"` 		// directory for LevelDB instances
	Transport       string        `json:"transport"`    // not used
	Codec           string        `json:"codec"`
	ReplyWhenCommit bool          `json:"reply_when_commit"` // reply to bench when request is committed, instead of executed
	Adaptive        bool          `json:"adaptive"`          // adaptive leader change, if true paxos forward request to current leader
	Interval        int           `json:"interval"`          // interval for leader change, 0 means immediate
	BackOff         int           `json:"backoff"`           // random backoff interval
	Thrifty         bool          `json:"thrifty"`           // only send messages to a quorum
	ChanBufferSize  int           `json:"chan_buffer_size"`
	BufferSize      int           `json:"buffer_size"`
	BalGossipInt    int64         `json:"balance_gossip_interval"` //Interval between balance data gossip messages in ms
	OverldThrshld   float64       `json:"overload_threshold"` //how many percent over even distribution is considered to be overlaod
	TX_lease	    int       	  `json:"tx_lease_duration"` //how long is tx lease (in ms) preventing other nodes from stealing
	handoverN	    int       	  `json:"handoverN"` //how many request we need before making polite handover decision


	// for future implementation
	// Batching bool `json:"batching"`
	// Consistency string `json:"consistency"`
}

func MakeDefaultConfig() Config {
	dbDir := make([]string, 1)
	dbDir[0] = "/tmp/lvldb/"
	config := new(Config)
	config.ID = "1.1"
	config.Addrs = map[ID]string{"1.1": "127.0.0.1:" + strconv.Itoa(PORT)}
	config.HTTPAddrs = map[ID]string{"1.1": "http://localhost:" + strconv.Itoa(HTTP_PORT)}
	config.Quorum = "fgrid"
	config.LevelDBDir = dbDir
	config.Adaptive = true
	config.ChanBufferSize = CHAN_BUFFER_SIZE
	config.BufferSize = BUFFER_SIZE
	config.Transport = "chan"
	config.Codec = "gob"
	config.BalGossipInt = 1000
	config.OverldThrshld = 0.05
	config.TX_lease = 1000
	config.RS = 1
	config.handoverN = 5
	return *config
}

// NewConfig creates config object with given node id and config file path
func NewConfig(id ID) Config {
	config := new(Config)
	config.ID = id
	err := config.Load()
	if err != nil {
		log.Fatalln(err)
	}
	return *config
}

// String is implemented to print the config
func (c *Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Errorln(err)
	}
	return string(config)
}

// Load load configurations from config file in JSON format
func (c *Config) Load() error {
	file, err := os.Open(*config)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	return decoder.Decode(c)
}

// Save save configurations to file in JSON format
func (c *Config) Save() error {
	file, err := os.Create(*config)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
