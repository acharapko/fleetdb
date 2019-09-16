package db_node

import (
	"time"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/replication/wpaxos"
	"github.com/acharapko/fleetdb/log"
	"sort"
	"sync"
	"os"
	"runtime/pprof"
	"github.com/acharapko/fleetdb/kv_store"
	"github.com/acharapko/fleetdb/ids"
	"github.com/acharapko/fleetdb/config"
	"github.com/acharapko/fleetdb/utils"
)


type DBNode struct {
	*wpaxos.Replica

	//data distribution data
	balanceCount       map[ids.ID] int
	loadFactor		   map[ids.ID] float64
	targetBalance	   float64
	//zone distances
	pingDistances      map[ids.ID] int
	pingZonesDistances map[uint8] int

	//replication group zone
	rgz				  []uint8

	//stats map[string] hitstat
	memprofile string

	//TODO: refactor load factor and distance calculation to use channel sync instead?
	dbnodelock sync.RWMutex
}

func NewDBNode() *DBNode {
	db := new(DBNode)
	log.Infof("Creating DBNode id = %v \n", ids.GetID())
	db.balanceCount = make(map[ids.ID]int)
	db.loadFactor = make(map[ids.ID]float64)
	db.pingDistances = make(map[ids.ID]int)
	db.pingZonesDistances = make(map[uint8]int)
	db.Replica = wpaxos.NewReplica()

	//fleetdb utils
	log.Infof("Registering fleetdb utils at node %v \n", ids.GetID())
	db.Register(GossipBalance{}, db.handleGossipBalance)
	db.Register(ProximityPingRequest{}, db.handleProximityPingRequest)
	db.Register(ProximityPingResponse{}, db.handleProximityPingResponse)
	db.Register(fleetdb.Request{}, db.HandleRequest)
	db.Register(fleetdb.Transaction{}, db.handleTransaction)
	db.Register(wpaxos.LeaderChange{}, db.handleLeaderChange)
	db.Register(wpaxos.Prepare{}, db.handlePrepareDBNode)
	db.startBalanceGossip() // start gossiping about data balance in the system
	db.startProximityGossip() // start gossiping about node proximity
	//send gossip message
	m := ProximityPingRequest{db.ID(),  time.Now().UnixNano()}
	db.Broadcast(&m)
	return db
}

func (db *DBNode) SetMemProfile(memprofile string) {
	db.memprofile = memprofile
}

func (db *DBNode) startBalanceGossip() {
	log.Infof("Starting balance gossip at node %v \n", ids.GetID())
	ticker := time.NewTicker(time.Duration(config.Instance.BalGossipInt) * time.Millisecond)

	go func() {
		for {
			select {
				case <- ticker.C:
					//send gossip message
					db.dbnodelock.Lock()
					countKeys := db.CountKeys()
					db.balanceCount[db.ID()] = countKeys
					db.dbnodelock.Unlock()

					db.Broadcast(&GossipBalance{countKeys, db.ID()})

					log.Debugf("Memprofile file: %s\n", db.memprofile)
					if db.memprofile != "" {
						f, err := os.Create(db.memprofile)
						if err != nil {
							log.Fatal(err)
						}
						pprof.WriteHeapProfile(f)
						f.Close()
					}
			}
		}
	}()
}

func (db *DBNode) startProximityGossip() {
	log.Infof("Starting proximity gossip at node %v \n", ids.GetID())
	ticker := time.NewTicker(time.Duration(config.Instance.BalGossipInt) * time.Millisecond)
	go func() {
		for {
			select {
			case <- ticker.C:
				//send gossip message
				db.Broadcast(&ProximityPingRequest{db.ID(), time.Now().UnixNano()})
			}
		}
	}()
}

func (db *DBNode) computeReplicationGroupZones(zone uint8) []uint8{
	db.dbnodelock.Lock()
	defer db.dbnodelock.Unlock()

	rgz := make([]uint8, config.Instance.RS)
	rgz[0] = zone //our zone is always in our region

	distToZone := map[int][]uint8{}
	var dist []int
	for z, dist := range db.pingZonesDistances {
		distToZone[dist] = append(distToZone[dist], z) //adding zone to this distance
	}
	for z := range distToZone {
		dist = append(dist, z)
	}
	//sorting distances
	sort.Sort(sort.IntSlice(dist))

	zleft := config.Instance.RS - 1
	//iterate through distances
	for _, d := range dist {
		for _, z := range distToZone[d] {
			if zleft > 0 {
				rgz[config.Instance.RS - zleft] = z
				zleft--
			}
		}
	}
	return rgz
}

func (db *DBNode) GetReplicationGroupZones(zone uint8) []uint8  {
	return db.rgz
}


//fleetdb Message Handlers

func (db *DBNode) handleGossipBalance(m GossipBalance) {
	db.dbnodelock.Lock()
	log.Debugf("GossipBalance ===[%v]=== @ Replica %s\n", m, m.From)
	db.balanceCount[m.From] = m.Items
	db.dbnodelock.Unlock()
	db.computeLoadFactor(m.From)
}

func (db *DBNode) handleProximityPingRequest(m ProximityPingRequest) {
	log.Debugf("ProximityPingRequest Replica %s ===[%v]=== \n", m.From, m)
	db.Send(m.From, &ProximityPingResponse{db.ID(), m.TimeSent})
}

func (db *DBNode) handleProximityPingResponse(m ProximityPingResponse) {
	log.Debugf("ProximityPingResponse Replica %s ===[%v]=== \n", m.From, m)
	//TODO: compute ping for that node and compute ping distance between availability zones
	tdiff := time.Now().UnixNano() - m.TimeSent
	db.dbnodelock.Lock()
	db.pingDistances[m.From] = int(tdiff)
	//TODO: Maybe compute average distance to zone?
	db.pingZonesDistances[m.From.Zone()] = int(tdiff)
	db.dbnodelock.Unlock()
	db.rgz = db.computeReplicationGroupZones(db.ID().Zone())
}



//utils

/**
	This computes how loaded this nodes is with data compared to other nodes in the system.
 */
func (db *DBNode) computeLoadFactor(id ids.ID) {
	db.dbnodelock.Lock()
	defer db.dbnodelock.Unlock()
	var totalCount int
	for _, v := range db.balanceCount {
		totalCount += v
	}
	db.targetBalance = 1.0 / float64(len(db.balanceCount))
	db.loadFactor[id] = float64(db.balanceCount[id]) / float64(totalCount)
	db.loadFactor[db.ID()] = float64(db.balanceCount[db.ID()]) / float64(totalCount) //recompute our own load factor based on new data
	log.Infof("load Factor @ Replica %s: %f (%d items) (target=%f)\n", id, db.loadFactor[id], db.balanceCount[id], db.targetBalance)
	log.Infof("load Factor @ Replica %s: %f (%d items) (target=%f)\n", db.ID(), db.loadFactor[db.ID()], db.balanceCount[db.ID()], db.targetBalance)
}

func (db *DBNode) computeOwnLoadFactor() {
	db.dbnodelock.Lock()
	defer db.dbnodelock.Unlock()
	var totalCount int
	for _, v := range db.balanceCount {
		totalCount += v
	}
	db.targetBalance = 1.0 / float64(len(db.balanceCount))
	db.loadFactor[db.ID()] = float64(db.balanceCount[db.ID()]) / float64(totalCount) //recompute our own load factor based on new data
	log.Infof("load Factor @ Replica %s: %f (%d items) (target=%f)\n", db.ID(), db.loadFactor[db.ID()], db.balanceCount[db.ID()], db.targetBalance)
}


func (db *DBNode) processLeaderChange(to ids.ID, p *wpaxos.Paxos) {
	db.dbnodelock.Lock()
	loadFctr := db.loadFactor[to]
	db.dbnodelock.Unlock()

	if to.Zone() != db.ID().Zone() {
		log.Debugf("Process Leader (Key=%s, table=%s) Change @ %s: to %s balance = %f, overld = %f\n", string(p.Key), p.Table, db.ID(), to, loadFctr, config.Instance.OverldThrshld)
		//we are changing zone.
		//see if we can have a better node for balance
		if loadFctr - config.Instance.OverldThrshld > db.targetBalance {
			//we can find a better node in the same replication region
			zones := db.computeReplicationGroupZones(to.Zone()) //this is our best guess for the replication region now
			//log.Debugf("Process Leader: Got Zones %v\n", zones)
			db.dbnodelock.RLock()
			for id, lf := range db.loadFactor {
				if utils.Uint8InSlice(id.Zone(), zones) && loadFctr > lf {
					to = id
					break
				}
			}
			db.dbnodelock.RUnlock()
		}

		db.Send(to, &wpaxos.LeaderChange{
			Key:    p.Key,
			Table:  *p.Table,
			To:     to,
			From:   db.ID(),
			Ballot: p.Ballot(),
		})
	}
}

type evictionNotice struct {
	key   kv_store.Key
	table string
	dest  ids.ID
}

func (db *DBNode) findEvictKey(table string) *evictionNotice {
	log.Debugf("Find Evict Keys in table %s @ %v \n", table, db.ID())

	//we will do the ugly and evict least accessed key
	//of course the is flawed, since the counter for
	//different keys reset at different times.
	//so likely we are going to evict key that just recently
	//reset the counter. To prevent this we use normalized stat
	tbl := db.GetTable(table)
	lak := tbl.FindLeastUsedKey()
	if lak != nil {
		//now find a good home for the poor evicted key ;)
		log.Debugf("Found Evict Key @ %v: %s \n", db.ID(), lak)
		//first, lets check if this region has some room
		db.dbnodelock.RLock()
		for id, lf := range db.loadFactor {
			thisRR := utils.Uint8InSlice(id.Zone(), db.rgz)
			if id != db.ID() && thisRR && lf - config.Instance.OverldThrshld < db.targetBalance {
				//we found a home!
				log.Infof("SAME REGION (lf = %f) EVICTION TO %s \n",lf, id)
				tbl.MarkKeyEvicting(lak)
				db.dbnodelock.RUnlock()
				return &evictionNotice{key: lak, table: table, dest: id}
			}
		}
		db.dbnodelock.RUnlock()

		//if we got to here, there was no suitable place in the region.
		//lets find something as close as possible
		distToZone := map[int][]uint8{}
		var dist []int
		db.dbnodelock.RLock()
		for z, dist := range db.pingZonesDistances {
			distToZone[dist] = append(distToZone[dist], z) //adding zone to this distance
		}
		db.dbnodelock.RUnlock()
		for z := range distToZone {
			dist = append(dist, z)
		}
		//sorting distances
		sort.Sort(sort.IntSlice(dist))

		for _, d := range dist {
			for _, z := range distToZone[d] {
				//log.Debugf("Checking zone %d to evict %s\n", z, string(lak))
				difReplGrp := utils.Uint8InSlice(z, db.rgz)
				if !difReplGrp {
					//z is in different replication region
					db.dbnodelock.RLock()
					for id, _ := range db.pingDistances {
						if id.Zone() == z {
							log.Infof("REMOTE REGION EVICTION TO %s \n", id)
							tbl.MarkKeyEvicting(lak)
							db.dbnodelock.RUnlock()
							return &evictionNotice{key: lak, table: table, dest: id}
						}
					}
					db.dbnodelock.RUnlock()
				}
			}
		}
	}
	return nil
}

func (db *DBNode) EvictKey(table string) {
	log.Debugf("Replica %s evicting key\n", db.ID())
	db.dbnodelock.RLock()
	lf := db.loadFactor[db.ID()]
	tb := db.targetBalance
	db.dbnodelock.RUnlock()
	log.Infof("Considering Eviction @ %v: lf= %f, target=%f \n", db.ID(), lf, tb)
	//this ugly loop tells how many keys to evict. The bigger misbalance causes more evictions
	evictedCount:=0
	for i := tb; i < lf; i += config.Instance.OverldThrshld {
		if lf - config.Instance.OverldThrshld > tb {
			evictNotice := db.findEvictKey(table)
			if evictNotice != nil {
				p := db.GetPaxos(evictNotice.key, evictNotice.table)
				if p != nil {
					evictedCount++
					log.Debugf("Replica %s evicting key=%s to %s \n", db.ID(), string(evictNotice.key), evictNotice.dest)
					db.Send(evictNotice.dest, &wpaxos.LeaderChange{
						Key:    evictNotice.key,
						Table:  *p.Table,
						To:     evictNotice.dest,
						From:   db.ID(),
						Ballot: p.Ballot(),
					})
				}
			}
		}
	}
	db.dbnodelock.Lock()
	db.balanceCount[db.ID()] -= evictedCount
	db.dbnodelock.Unlock()
	db.computeOwnLoadFactor()
}

/**
	Handles
 */

func (db *DBNode) handlePrepareDBNode(m wpaxos.Prepare) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, db.ID())
	db.Replica.HandlePrepare(m)
	p := db.GetPaxos(m.Key, m.Table)
	if !p.Active {
		log.Debugf("Removing Stats for key=%s", string(m.Key))
		tbl := db.GetTable(m.Table)
		tbl.RemoveStats(m.Key)
	}

}

//Request
func (db *DBNode) HandleRequest(m fleetdb.Request) {

	log.Debugf("DBNode %s received %v\n", db.ID(), m)
	k := m.Command.Key

	p := db.GetPaxosByCmd(m.Command)
	if p != nil && config.Instance.Adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			db.Replica.HandleRequest(m)
			tbl := db.GetTable(m.Command.Table)
			tbl.InitStat(k)
			to := tbl.HitKey(k, m.Command.ClientID, m.Timestamp)

			if to.Zone() != db.ID().Zone() {
				db.processLeaderChange(ids.ID(to), p)
			}
			db.dbnodelock.RLock()
			lf := db.loadFactor[db.ID()]
			tb := db.targetBalance
			db.dbnodelock.RUnlock()
			if lf > tb {
				go db.EvictKey(m.Command.Table)
			}
		} else {
			go db.Forward(p.Leader(), m)
		}
	} else {
		db.Replica.HandleRequest(m)
	}

}


func (db *DBNode) handleTransaction(m fleetdb.Transaction) {

	log.Debugf("DBNode %s received TX %v {%v}\n", db.ID(), m.TxID, m)
	//first we check what Keys we have and run phase2-3 with TX_LEASE command
	//then we run phase-1 to get remaining keys
	//when we get they Key, we immediately run phase2-3 with TX_LEASE
	//once all keys are stolen we run phase2-3 to get the TX-values
	//if success we Commit
	//if not, we abort
	for _, c := range m.Commands {
		tbl := db.GetTable(c.Table)
		tbl.InitStat(c.Key)
		tbl.HitKey(c.Key, c.ClientID, m.Timestamp)
	}
	db.Replica.HandleTransaction(m)

}


func (db *DBNode) pickNodeForTX(commands [] kv_store.Command) ids.ID {

	leaderMap := make(map[ids.ID]int)
	for _, cmd := range commands {
		p := db.GetPaxosByCmd(cmd)
		if p != nil {
			leaderMap[db.ID()]++
		}
	}
	id := db.ID()
	max := 0
	for k, v := range leaderMap {
		if v > max {
			max = v
			id = k
		}
	}
	return id
}


//Leader Chnage
func (db *DBNode) handleLeaderChange(m wpaxos.LeaderChange) {
	log.Debugf("Replica %s Change to %s ===[%v]===>>> Replica %s\n", m.From, m.To, m, db.ID())
	if m.To == db.ID() {
		//log.Debugf("Replica %s : change leader of Key %s\n", r.ID(), string(m.Key))

		//we received leader change.
		//check if we are overloaded. If we are, initiate handover of some key
		//over to an adjacent region.
		//this is a pressure mechanism. is this node is under pressure, we need to move it along
		//the rest of the system.
		go db.EvictKey(m.Table)

		p := db.GetPaxos(m.Key, m.Table)

		p.GetAccessToken()
		defer p.ReleaseAccessToken()

		p.P1a()

	}
}