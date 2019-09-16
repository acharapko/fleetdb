package config

import (
	"encoding/json"
	"flag"
	"os"
	"strconv"

	"github.com/acharapko/fleetdb/log"
	"github.com/acharapko/fleetdb/ids"
)

var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")

var Instance *Config
// default values
const (
	PORT      = 1735
	HTTP_PORT = 8080
	BUFFER_SIZE      = 1024 * 1
	// TODO merge below two value with with config
	CHAN_BUFFER_SIZE = 1024 * 1
)

type Config struct {
	Addrs           map[ids.ID]string 	`json:"address"`      		// address for node communication
	HTTPAddrs       map[ids.ID]string 	`json:"http_address"` 		// address for bench server communication
	Quorum          string        		`json:"quorum"`       		// type of the quorums
	F               int           		`json:"f"`            		// number of failure zones in general grid quorums
	RS              int          		`json:"rs"`            		// number of failure zones in a replication region
	LevelDBDir      []string      		`json:"leveldb"` 			// directory for LevelDB instances
	Transport       string        		`json:"transport"`    		// not used
	Codec           string        		`json:"codec"`
	ReplyWhenCommit bool        		`json:"reply_when_commit"`  // reply to bench when request is committed, instead of executed
	Adaptive        bool        		`json:"adaptive"`          	// adaptive leader change, if true paxos forward request to current leader
	Interval        int          		`json:"interval"`          	// interval for leader change, 0 means immediate
	BackOff         int           		`json:"backoff"`           	// random backoff interval
	Thrifty         bool          		`json:"thrifty"`           	// only send messages to a quorum
	ChanBufferSize  int           		`json:"chan_buffer_size"`
	BufferSize      int           		`json:"buffer_size"`
	BalGossipInt    int64         		`json:"balance_gossip_interval"`//Interval between balance data gossip messages in ms
	OverldThrshld   float64       		`json:"overload_threshold"` 	//how many percent over even distribution is considered to be overlaod
	TX_lease	    int       	  		`json:"tx_lease_duration"` 	//how long is tx lease (in ms) preventing other nodes from stealing
	HandoverN	    int       	  		`json:"handoverN"` 			//how many request we need before making polite handover decision
	Migration_maj   float64    	  		`json:"migration_majority"` //http max idle connection per host
	MaxIdleCnx	    int       	  		`json:"max_idle_connections"` //http max idle connection per host
	CoGAlpha		float32				`json:"cog_alpha"`
	CoGMinNumHits	int					`json:"cog_min_number_hits"`


	// for future implementation
	// Batching bool `json:"batching"`
	// Consistency string `json:"consistency"`
}

func MakeDefaultConfig() Config {
	dbDir := make([]string, 1)
	dbDir[0] = "/tmp/lvldb/"
	config := new(Config)
	config.Addrs = map[ids.ID]string{"1.1": "127.0.0.1:" + strconv.Itoa(PORT)}
	config.HTTPAddrs = map[ids.ID]string{"1.1": "http://localhost:" + strconv.Itoa(HTTP_PORT)}
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
	config.HandoverN = 5
	config.MaxIdleCnx = 100
	config.Migration_maj = 0.05
	config.CoGAlpha = 0.1
	config.CoGMinNumHits = 3
	return *config
}

func init() {
	log.Infof("Loading configuration from %s \n", *configFile)
	Instance = new(Config)
	err := Instance.load()
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *Config) GetZoneIds() []uint8 {
	zones := make([]uint8, 0)
	for id, _ := range c.Addrs {
		idInZones := false
		for _, z := range zones {
			if z == id.Zone() {
				idInZones = true
				break
			}
		}
		if !idInZones {
			zones = append(zones, id.Zone())
		}
	}
	return zones
}

// String is implemented to print the config
func (c *Config) String() string {
	config, err := json.Marshal(c)
	if err != nil {
		log.Errorln(err)
	}
	return string(config)
}

// load load configurations from config file in JSON format
func (c *Config) load() error {
	file, err := os.Open(*configFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(file)
	return decoder.Decode(c)
}

// Save save configurations to file in JSON format
func (c *Config) Save() error {
	file, err := os.Create(*configFile)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	return encoder.Encode(c)
}
