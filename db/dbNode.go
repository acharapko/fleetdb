package db

import (
	"time"
	"github.com/acharapko/fleetdb"
	"github.com/acharapko/fleetdb/wpaxos2"
	"github.com/acharapko/fleetdb/log"
	"sort"
	"sync"
	"os"
	"runtime/pprof"
)


type DBNode struct {
	*wpaxos2.Replica

	//data distribution data
	balanceCount       map[fleetdb.ID] int
	loadFactor		   map[fleetdb.ID] float64
	targetBalance	   float64
	//zone distances
	pingDistances      map[fleetdb.ID] int
	pingZonesDistances map[int] int

	//replication group zone
	rgz				  []int

	stats map[string] hitstat
	memprofile string

	//TODO: refactor load factor and distance calculation to use channel sync instead?
	sync.RWMutex
}

func NewDBNode(config fleetdb.Config) *DBNode {
	db := new(DBNode)
	db.balanceCount = make(map[fleetdb.ID]int)
	db.loadFactor = make(map[fleetdb.ID]float64)
	db.pingDistances = make(map[fleetdb.ID]int)
	db.pingZonesDistances = make(map[int]int)
	db.Replica = wpaxos2.NewReplica(config)
	db.stats = make(map[string] hitstat)
	//fleetdb utils
	db.Register(GossipBalance{}, db.handleGossipBalance)
	db.Register(ProximityPingRequest{}, db.handleProximityPingRequest)
	db.Register(ProximityPingResponse{}, db.handleProximityPingResponse)
	db.Register(fleetdb.Request{}, db.HandleRequest)
	db.Register(fleetdb.Transaction{}, db.handleTransaction)
	db.Register(wpaxos2.LeaderChange{}, db.handleLeaderChange)
	db.Register(wpaxos2.Prepare{}, db.handlePrepareDBNode)
	db.startBalanceGossip(config)
	db.startProximityGossip(config)
	//send gossip message
	m := ProximityPingRequest{db.ID(),  time.Now().UnixNano()}
	db.Broadcast(&m)
	return db
}

func (db *DBNode) initStat(key fleetdb.Key) {
	db.Lock()
	defer db.Unlock()
	if _, exists := db.stats[key.B64()]; !exists {
		log.Debugf("Init Stat for Key %s\n", key)
		db.stats[key.B64()] = newStat()
	}
}

func (db *DBNode) removeStats(key fleetdb.Key) {
	db.Lock()
	defer db.Unlock()
	if _, exists := db.stats[key.B64()]; exists {
		//log.Debugf("Remove Stat for Key %s\n", key)
		delete(db.stats, key.B64())
	}
}

func (db *DBNode) SetMemProfile(memprofile string) {
	db.memprofile = memprofile
}

func (db *DBNode) startBalanceGossip(config fleetdb.Config) {
	ticker := time.NewTicker(time.Duration(config.BalGossipInt) * time.Millisecond)

	go func() {
		for {
			select {
				case <- ticker.C:
					//send gossip message
					db.RLock()
					countKeys := len(db.stats)
					db.RUnlock()
					db.Broadcast(&GossipBalance{countKeys, db.ID()})
					db.Lock()
					db.balanceCount[db.ID()] = countKeys
					db.Unlock()
					db.computeLoadFactor(db.ID())


					log.Debugf("Memprofile file: \n", db.memprofile)
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

func (db *DBNode) startProximityGossip(config fleetdb.Config) {
	ticker := time.NewTicker(time.Duration(config.BalGossipInt) * time.Millisecond)
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

func (db *DBNode) computeReplicationGroupZones(zone int) []int{
	db.Lock()
	defer db.Unlock()

	rgz := make([]int, db.Config().RS)
	rgz[0] = zone //our zone is always in our region

	distToZone := map[int][]int{}
	var dist []int
	for z, dist := range db.pingZonesDistances {
		distToZone[dist] = append(distToZone[dist], z) //adding zone to this distance
	}
	for z := range distToZone {
		dist = append(dist, z)
	}
	//sorting distances
	sort.Sort(sort.IntSlice(dist))

	zleft := db.Config().RS - 1
	//iterate through distances
	for _, d := range dist {
		for _, z := range distToZone[d] {
			if zleft > 0 {
				rgz[db.Config().RS - zleft] = z
				zleft--
			}
		}
	}
	return rgz
}

func (db *DBNode) GetReplicationGroupZones(zone int) []int  {
	return db.rgz
}


//fleetdb Message Handlers

func (db *DBNode) handleGossipBalance(m GossipBalance) {
	db.Lock()
	log.Debugf("GossipBalance ===[%v]=== @ Replica %s\n", m, m.From)
	db.balanceCount[m.From] = m.Items
	db.Unlock()
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
	db.Lock()
	db.pingDistances[m.From] = int(tdiff)
	//TODO: Maybe compute average distance to zone?
	db.pingZonesDistances[m.From.Zone()] = int(tdiff)
	db.Unlock()
	db.rgz = db.computeReplicationGroupZones(db.ID().Zone())
}



//utils

/**
	This computes how loaded this nodes is with data compared to other nodes in the system.
 */
func (db *DBNode) computeLoadFactor(id fleetdb.ID) {
	db.Lock()
	defer db.Unlock()
	var totalCount int
	for _, v := range db.balanceCount {
		totalCount += v
	}
	db.targetBalance = 1.0 / float64(len(db.balanceCount))
	db.loadFactor[id] = float64(db.balanceCount[id]) / float64(totalCount)
	log.Infof("Load Factor @ Replica %s: %f (%d items) (target=%f)\n", id, db.loadFactor[id], db.balanceCount[id], db.targetBalance)
}


func (db *DBNode) processLeaderChange(to fleetdb.ID, p *wpaxos2.Paxos) {
	db.RLock()
	loadFctr := db.loadFactor[to]
	db.RUnlock()

	if to.Zone() != db.ID().Zone() {
		log.Debugf("Process Leader Change @ %s: to (%s) balance = %f, overld = %f\n", db.ID(), to,  loadFctr, db.Config().OverldThrshld)
		//we are changing zone.
		//see if we can have a better node for balance
		if loadFctr - db.Config().OverldThrshld > db.targetBalance {
			//we can find a better node in the same replication region
			zones := db.computeReplicationGroupZones(to.Zone()) //this is our best guess for the replication region now
			//log.Debugf("Process Leader: Got Zones %v\n", zones)
			for id, lf := range db.loadFactor {
				if fleetdb.IntInSlice(id.Zone(), zones) && loadFctr > lf {
					to = id
					break
				}
			}
		}

		db.Send(to, &wpaxos2.LeaderChange{
			Key:    p.Key,
			To:     to,
			From:   db.ID(),
			Ballot: p.Ballot(),
		})
	}
}

type evictionNotice struct {
	key fleetdb.Key
	dest fleetdb.ID
}

func (db *DBNode) findEvictKey() *evictionNotice {
	log.Debugf("Find Evict Keys @ %v \n", db.ID())

	//we will do the ugly and evict least accessed key
	//of course the is flawed param, since the counter for
	//different keys reset at different times.
	//so likely we are going to evict key that just recently
	//reset the counter. To prevent this we use normalized stat
	var lak fleetdb.Key
	timesused := int64(^uint64(0) >> 1) //max int
	db.Lock()
	for k, hits := range db.stats {
		if !hits.Evicting() {
			lt := hits.LastReqTime()
			//log.Debugf("lt = %d \n", lt)
			if lt < timesused && lt > 0 {
				lak = fleetdb.KeyFromB64(k)
				timesused = lt
			}
		}
	}
	db.Unlock()
	if lak != nil {
		//now find a good home for the poor evicted key ;)
		log.Debugf("Found Evict Key @ %v: %s \n", db.ID(), lak)
		//first, lets check if this region has some room
		for id, lf := range db.loadFactor {
			thisRR := fleetdb.IntInSlice(id.Zone(), db.rgz)
			if id != db.ID() && thisRR && lf - db.Config().OverldThrshld < db.targetBalance {
				//we found a home!
				log.Infof("SAME REGION (lf = %f) EVICTION TO %s \n",lf, id)
				db.RLock()
				db.stats[lak.B64()].MarkEvecting()
				db.RUnlock()
				return &evictionNotice{key: lak, dest: id}
			}
		}

		//if we got to here, there was no suitable place in the region.
		//lets find something as close as possible
		distToZone := map[int][]int{}
		var dist []int
		for z, dist := range db.pingZonesDistances {
			distToZone[dist] = append(distToZone[dist], z) //adding zone to this distance
		}
		for z := range distToZone {
			dist = append(dist, z)
		}
		//sorting distances
		sort.Sort(sort.IntSlice(dist))

		for _, d := range dist {
			for _, z := range distToZone[d] {
				//log.Debugf("Checking zone %d to evict %s\n", z, string(lak))
				difReplGrp := fleetdb.IntInSlice(z, db.rgz)
				if !difReplGrp {
					//z is in different replication region
					for id, _ := range db.pingDistances {
						if id.Zone() == z {
							log.Infof("REMOTE REGION EVICTION TO %s \n", id)
							db.RLock()
							db.stats[lak.B64()].MarkEvecting()
							db.RUnlock()
							return &evictionNotice{key: lak, dest: id}
						}
					}
				}
			}
		}
	}
	return nil
}

func (db *DBNode) EvictKey() {
	log.Debugf("Replica %s evicting key\n", db.ID())
	db.RLock()
	lf := db.loadFactor[db.ID()]
	tb := db.targetBalance
	db.RUnlock()
	log.Debugf("EvictKey lf= %f \n", lf)
	//this ugly loop tells how many keys to evict. The bigger misbalance causes more evictions
	for i := tb; i < lf; i += db.Config().OverldThrshld {
		if lf - db.Config().OverldThrshld > tb {
			evictNotice := db.findEvictKey()
			if evictNotice != nil {
				log.Debugf("Replica %s evicting key %s to %s \n", db.ID(), string(evictNotice.key), evictNotice.dest)
				p := db.GetPaxos(evictNotice.key)

				db.Send(evictNotice.dest, &wpaxos2.LeaderChange{
					Key:    evictNotice.key,
					To:     evictNotice.dest,
					From:   db.ID(),
					Ballot: p.Ballot(),
				})
			}
		}
	}
}

/**
	Handles
 */

func (db *DBNode) handlePrepareDBNode(m wpaxos2.Prepare) {
	//log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, r.ID())
	db.Replica.HandlePrepare(m)
	p := db.GetPaxos(m.Key)
	if !p.Active {
		db.removeStats(m.Key)
	}

}

//Request
func (db *DBNode) HandleRequest(m fleetdb.Request) {

	log.Debugf("DBNode %s received %v\n", db.ID(), m)
	k := m.Command.Key

	p := db.GetPaxos(k)
	if p != nil && db.Config().Adaptive {
		if p.IsLeader() || p.Ballot() == 0 {
			db.initStat(m.Command.Key)
			db.Replica.HandleRequest(m)
			db.RLock()
			to := db.stats[k.B64()].Hit(m.Command.ClientID, m.Timestamp)
			db.RUnlock()
			if to != "" {
				db.processLeaderChange(fleetdb.ID(to), p)
			}
		} else {
			go db.Forward(p.Leader(), m)
		}
	} else {
		go db.EvictKey()
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
		db.initStat(c.Key)
		db.Lock()
		db.stats[c.Key.B64()].Hit(c.ClientID, m.Timestamp)
		//db.stats[c.Key.B64()].HitWeight(c.ClientID, len(m.Commands) / 2 + 1, m.Timestamp)
		db.Unlock()
	}
	db.Replica.HandleTransaction(m)

}


func (db *DBNode) pickNodeForTX(commands [] fleetdb.Command) fleetdb.ID {

	leaderMap := make(map[fleetdb.ID]int)
	for _, cmd := range commands {
		p := db.GetPaxos(cmd.Key)
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
func (db *DBNode) handleLeaderChange(m wpaxos2.LeaderChange) {
	log.Debugf("Replica %s Change to %s ===[%v]===>>> Replica %s\n", m.From, m.To, m, db.ID())
	if m.To == db.ID() {
		//log.Debugf("Replica %s : change leader of Key %s\n", r.ID(), string(m.Key))

		//we received leader change.
		//check if we are overloaded. If we are, initiate handover of some key
		//over to an adjacent region.
		//this is a pressure mechanism. is this node is under pressure, we need to move it along
		//the rest of the system.
		go db.EvictKey()

		p := db.GetPaxos(m.Key)

		p.GetAccessToken()
		defer p.ReleaseAccessToken()

		p.P1a()

	}
}