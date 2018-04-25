package key_value

import (
	"errors"
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/acharapko/fleetdb/log"
	"strconv"
	"github.com/acharapko/fleetdb/config"
)

var (
	ErrStoreError = errors.New("LevelDB error")
	ErrStoreGetError = errors.New("LevelDB error during Get operation")
	ErrNotFound = errors.New("LevelDB: item not found")
	ErrStorePutError = errors.New("LevelDB error during Put operation")
	ErrStoreDelError = errors.New("LevelDB error during Delete operation")
)

// StateMachine interface provides execution of command against database
// the implementation should be thread safe
type Store interface {
	Execute(c Command) (Value, error)
}

// database maintains the key-value datastore
type database struct {
	lock 		*sync.RWMutex
	leveldbs    map[string] []*leveldb.DB  // map of leveldb shards
	cfg 		config.Config
}

// NewStore get the instance of LevelDB Wrapper
func NewStore(config config.Config) Store {
	db := new(database)
	db.lock = new(sync.RWMutex)
	db.cfg = config
	db.leveldbs = make(map[string] []*leveldb.DB)
	return db
}

func (db *database) getStore(name string) []*leveldb.DB{
	storedbs := db.leveldbs[name]
	if storedbs == nil {
		storedbs = make([]*leveldb.DB, 0)
		for i, dir := range db.cfg.LevelDBDir {
			lvlDBName := dir + "/" + name + "/" + strconv.Itoa(db.cfg.ID.Zone()) + "." + strconv.Itoa(db.cfg.ID.Node())+ "." + strconv.Itoa(i)
			lvldb, err := leveldb.OpenFile(lvlDBName,nil)
			if err != nil {
				log.Fatal("Error opening LevelDB store: " + lvlDBName)
			}
			storedbs = append(storedbs, lvldb)
		}
		db.leveldbs[name] = storedbs;
	}
	return storedbs

}


func (db *database) Execute(c Command) (Value, error) {
	//log.Debugf("Executing Command %v\n", c)
	storedbs := db.getStore(c.Table)
	lvldb := storedbs[c.Key.Bucket(len(storedbs))]
	switch c.Operation {
	case PUT:
		db.lock.Lock()
		defer db.lock.Unlock()
		err := lvldb.Put(c.Key, c.Value, nil)
		if err != nil {
			log.Errorln(err)
			return nil, ErrStorePutError
		}
		return nil, nil
	case GET:
		db.lock.Lock()
		defer db.lock.Unlock()
		v, err := lvldb.Get(c.Key, nil)
		if err != nil {
			if err.Error() == "leveldb: not found" {
				return nil, ErrNotFound
			}
			log.Errorln(err)
			return nil, ErrStoreGetError
		}
		log.Debugf("Execute GET against LevelDB: %s\n", v)
		return v, nil
	case DELETE:
		db.lock.Lock()
		defer db.lock.Unlock()
		err := lvldb.Delete(c.Key, nil)
		if err != nil {
			log.Errorln(err)
			return nil, ErrStoreDelError
		}
		return nil, nil
	case NOOP:
		//do nothing
		return nil, nil
	}
	return nil, ErrStoreError
}

func (db *database) String() string {

	return "LevelDB Database"
}