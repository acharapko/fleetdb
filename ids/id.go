package ids

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/acharapko/fleetdb/log"
)

var id = flag.String("id", "1.1", "ID in format of Zone.Node. Default 1.1")

// ID represents a generic identifier in format of Zone.Node
type ID string

// GetID gets the current id specified in flag variables
func GetID() ID {
	if !flag.Parsed() {
		log.Warningln("Using ID before parse flag")
	}
	return ID(*id)
}

func NewID(zone, node uint8) ID {
	if zone < 0 {
		zone = -zone
	}
	if node < 0 {
		node = -node
	}
	return ID(fmt.Sprintf("%d.%d", zone, node))
}

// Zone returns Zond ID component
func (i ID) Zone() uint8 {
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		return 0
	}
	s := strings.Split(string(i), ".")[0]
	zone, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Zone %s to int\n", s)
	}
	return uint8(zone)
}

// Node returns Node ID component
func (i ID) Node() uint8 {
	var s string
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		s = string(i)
	} else {
		s = strings.Split(string(i), ".")[1]
	}
	node, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Node %s to int\n", s)
	}
	return uint8(node)
}
