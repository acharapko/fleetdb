package fleetdb

import (
	"errors"
	"sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/acharapko/fleetdb/log"
	"encoding/base64"
	"strconv"
)

var (
	ErrStoreError = errors.New("LevelDB error")
	ErrStoreGetError = errors.New("LevelDB error during Get operation")
	ErrNotFound = errors.New("LevelDB: item not found")
	ErrStorePutError = errors.New("LevelDB error during Put operation")
	ErrStoreDelError = errors.New("LevelDB error during Delete operation")
)

type Value []byte

type Key []byte

func (k Key) B64() (string){
	return base64.StdEncoding.EncodeToString(k)
}

func (k Key) Bucket(numBuckets int) int {
	var tempByte byte;

	for _, b := range k {
		tempByte = tempByte ^ b
	}
	bucket := int(tempByte) % numBuckets
	return bucket
}

func KeyFromB64(k string) Key {
	kb, err := base64.StdEncoding.DecodeString(k)

	if err == nil {
		return kb
	}
	return nil
}


// StateMachine interface provides execution of command against database
// the implementation should be thread safe
type Store interface {
	Execute(c Command) (Value, error)
}

// database maintains the key-value datastore
type database struct {
	lock *sync.RWMutex
	// data  map[Keys]map[Version]Value
	leveldb []*leveldb.DB
	//sync.RWMutex
}

// NewStore get the instance of LevelDB Wrapper
func NewStore(config Config) Store {
	db := new(database)
	db.lock = new(sync.RWMutex)
	for i, dir := range config.LevelDBDir {
		lvlDBName := dir + "/" + strconv.Itoa(config.ID.Zone()) + "." + strconv.Itoa(config.ID.Node())+ "." + strconv.Itoa(i)
		lvldb, err := leveldb.OpenFile(lvlDBName,nil)
		if err != nil {
			log.Fatal("Error opening LevelDB store: " + lvlDBName)
		}
		db.leveldb = append(db.leveldb, lvldb)
	}
	return db
}


func (db *database) Execute(c Command) (Value, error) {
	//log.Debugf("Executing Command %v\n", c)
	lvldb := db.leveldb[c.Key.Bucket(len(db.leveldb))]
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