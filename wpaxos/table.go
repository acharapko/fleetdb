package wpaxos

import (
	"github.com/acharapko/fleetdb/log"
	"sync"
	"github.com/acharapko/fleetdb/key_value"
	"github.com/acharapko/fleetdb/ids"
)

type Table struct {
	TableName 	*string
	sync.RWMutex
	stats 		map[string] hitstat
	paxi  		map[string]*Paxos
}

func NewTable(table string) *Table {
	t := new(Table)
	t.TableName = &table
	t.stats = make(map[string] hitstat)
	t.paxi = make(map[string]*Paxos)
	return t
}

func (t *Table) GetPaxos(key key_value.Key) *Paxos {
	t.Lock()
	defer t.Unlock()

	return t.paxi[key.B64()]
}

func (t *Table) Init(key key_value.Key, r *Replica) {
	t.Lock()
	defer t.Unlock()

	if _, exists := t.paxi[key.B64()]; !exists {
		log.Debugf("Init Key %s in table %s\n", key, *t.TableName)
		t.paxi[key.B64()] = NewPaxos(r, key, t.TableName)
	}
}

func (t *Table) CountKeys() int {
	/*sum := 0
	t.RLock()
	defer t.RUnlock()

	for _, paxos := range t.paxi {
		if paxos.Active {
			sum++
		}
	}
	return sum*/
	return len(t.stats)
}


func (t *Table) InitStat(key key_value.Key) {
	t.Lock()
	defer t.Unlock()
	k := key.B64()
	if _, exists := t.stats[k]; !exists {
		log.Debugf("Init Stat for Key %s\n", key)
		t.stats[k] = NewStat()
	}
}

func (t *Table) RemoveStats(key key_value.Key) {
	t.Lock()
	defer t.Unlock()
	k := key.B64()
	if _, exists := t.stats[k]; exists {
		//log.Debugf("Remove Stat for Key %s\n", key)
		delete(t.stats, k)
	}
}

func (t *Table) HitKey(key key_value.Key, clientID ids.ID, timestamp int64) ids.ID {
	t.RLock()
	defer t.RUnlock()
	return t.stats[key.B64()].Hit(clientID, timestamp)
}

func (t *Table) FindLeastUsedKey() key_value.Key {
	t.Lock()
	defer t.Unlock()
	var lak key_value.Key
	timesused := int64(^uint64(0) >> 1) //max int
	for k, hits := range t.stats {
		if !hits.Evicting() {
			lt := hits.LastReqTime()
			//log.Debugf("lt = %d \n", lt)
			if lt < timesused && lt > 0 {
				lak = key_value.KeyFromB64(k)
				timesused = lt
			}
		}
	}
	return lak
}

func (t *Table) MarkKeyEvicting(k key_value.Key) {
	t.Lock()
	defer t.Unlock()
	t.stats[k.B64()].MarkEvicting()
}