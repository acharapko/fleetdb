package wpaxos

import (
	"github.com/acharapko/fleetdb/log"
	"sync"
	"github.com/acharapko/fleetdb/kv_store"
	"github.com/acharapko/fleetdb/ids"
)

var (
	NANOSECONDS_PER_SECOND = int64(1000000000)
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

func (t *Table) GetPaxos(key kv_store.Key) *Paxos {
	t.Lock()
	defer t.Unlock()

	return t.paxi[key.B64()]
}

func (t *Table) Init(key kv_store.Key, r *Replica) {
	t.Lock()
	defer t.Unlock()

	if _, exists := t.paxi[key.B64()]; !exists {
		log.Debugf("Init Key %s in table %s\n", key, *t.TableName)
		t.paxi[key.B64()] = NewPaxos(r, key, t.TableName)
	}
}

func (t *Table) CountKeys() int {
	return len(t.stats)
}


func (t *Table) InitStat(key kv_store.Key) {
	t.Lock()
	defer t.Unlock()
	k := key.B64()
	if _, exists := t.stats[k]; !exists {
		log.Debugf("Init Stat for Key %s\n", key)
		t.stats[k] = NewStat()
	}
}

func (t *Table) RemoveStats(key kv_store.Key) {
	t.Lock()
	defer t.Unlock()
	k := key.B64()
	if _, exists := t.stats[k]; exists {
		log.Debugf("Remove Stat for Key %s\n", key)
		delete(t.stats, k)
	}
}

func (t *Table) HitKey(key kv_store.Key, clientID ids.ID, timestamp int64) ids.ID {
	t.RLock()
	defer t.RUnlock()
	return t.stats[key.B64()].Hit(clientID, timestamp)
}


func (t *Table) FindLeastUsedKey() kv_store.Key {
	t.Lock()
	defer t.Unlock()
	var lak kv_store.Key
	var tempKey kv_store.Key
	timeUsed := int64(^uint64(0) >> 1) //max int
	for k, hits := range t.stats {
		lastUsedTime := hits.LastReqTime()
		if !hits.Evicting() && lastUsedTime < timeUsed && lastUsedTime > NANOSECONDS_PER_SECOND {
			tempKey = kv_store.KeyFromB64(k)
			p := t.paxi[tempKey.B64()]
			if p != nil  {
				hasLease := p.HasTXLease(0)
				if len(p.requests) == 0 && !hasLease {
					lak = tempKey
					timeUsed = lastUsedTime
				}
			}
		}
	}

	return lak
}

func (t *Table) MarkKeyEvicting(k kv_store.Key) {
	t.Lock()
	defer t.Unlock()
	stat := t.stats[k.B64()]
	if stat != nil {
		stat.MarkEvicting()
	}
}