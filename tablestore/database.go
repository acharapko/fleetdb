package tablestore

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/google/uuid"
	"sync"
	"fmt"
	"io"
	"bytes"
)

// StateMachine interface provides execution of command against database
// the implementation should be thread safe
type Store interface {
	Save(tableid uuid.UUID, items []KVItem) error
	GetPartition(tableid uuid.UUID, partitionKeys []FleetDBValue, clusteringKyes []FleetDBValue, clusteringComparators []ComparatorType, columns []string,  tableSpec FleetDBTableSpec) ([]Row, error)
}

// database maintains the key-value datastore
type database struct {
	lock 		*sync.RWMutex
	leveldbs    map[uuid.UUID] *leveldb.DB  // map of leveldb shards
}

// NewStore get the instance of LevelDB Wrapper
func NewStore() Store {
	db := new(database)
	db.lock = new(sync.RWMutex)
	db.leveldbs = make(map[uuid.UUID] *leveldb.DB)
	return db
}

func (db *database) getStore(tableid uuid.UUID) *leveldb.DB{
	lvldb := db.leveldbs[tableid]
	dir := "/tmp/lvldb" 
	if lvldb == nil {
			lvlDBName := dir + "/" + tableid.String()
			lvldb, _ = leveldb.OpenFile(lvlDBName,nil)
		db.leveldbs[tableid] = lvldb;
	}
	return lvldb

}

func (db *database) Save(tableid uuid.UUID, items []KVItem) error {
    storedb := db.getStore(tableid)
    var err error
    	batch := new(leveldb.Batch)
    	for _, item := range items{
    		if(item.Value == nil){
    			fmt.Printf("Deleting Key %s", item.Key)
    			batch.Delete(item.Key)
    		}else{
    			batch.Put(item.Key,item.Value)
    		}
    	}
		err  = storedb.Write(batch, nil)
		if err != nil {
			return err;
		}
	return nil
}


func buildKey(keyType uint8, key FleetDBValue) []byte{
	searchKey := make([]byte,0)
	searchKey = append(searchKey, keyType)
	coltype := key.getType()
	searchKey = append(searchKey, coltype.getVal())
	if coltype == Text{
    		searchKey = append(searchKey,[]byte(key.String()+ nullchar)...)
	}else{
    		searchKey = append(searchKey,key.Serialize()...)
	}
	return searchKey
}

func (db *database) GetPartition(tableid uuid.UUID, partitionKeys []FleetDBValue , clusteringKyes []FleetDBValue, clusteringComparators []ComparatorType, columns []string, tableSpec FleetDBTableSpec) ([]Row, error) {
	var rows []Row
	var err error
	storedb := db.getStore(tableid)
	oldCKeyvalues := []FleetDBValue{}
	oldColvalues := []FleetDBValue{}
	seqMap := make(map[uint8]string)
	for colName , columnSpec := range tableSpec.columnSpecs{
		seqMap[columnSpec.seqNo] = colName
	}
	
	valMap := make(map[string]FleetDBValue)
	oldValMap := make(map[string]FleetDBValue)
	searchKey := make([]byte,0)
	for _ , key := range partitionKeys{
		searchKey = append(searchKey,buildKey(PartitionType, key)...)
	}
		index := 0
		for index < len(clusteringComparators) &&  clusteringComparators[index] == EQUAL{
			searchKey = append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
			index++
		}
		isAtFirst := false
		iter := storedb.NewIterator(util.BytesPrefix(searchKey), nil)
		if(index < len(clusteringComparators)){
			switch(clusteringComparators[index]){
				case GREATERTHANEQUAL:
				lowerLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				iter = storedb.NewIterator(nil, nil)
				iter.Seek(lowerLimit)
				isAtFirst = true
				case GREATERTHAN:
				lowerLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				iter = storedb.NewIterator(nil, nil)
				iter.Seek(lowerLimit)
				isAtFirst = true
				for (isAtFirst || iter.Next()) && bytes.HasPrefix(iter.Key(), lowerLimit){
						isAtFirst = false
				}
				isAtFirst = true
				case LESSTHAN:
				lowerLimit := append(searchKey, ClusteringType)
				lowerLimit = append(lowerLimit, clusteringKyes[index].getType().getVal())
				upperLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				iter = storedb.NewIterator(&util.Range{Start: lowerLimit, Limit: upperLimit}, nil)
				//fmt.Printf("LessThanEqualLower: %s  LessThanEqualUpper: %s  ",  NewTextValue(bytes.NewReader(lowerLimit)).val, NewTextValue(bytes.NewReader(upperLimit)).val)
				
				case LESSTHANEQUAL:
				lowerLimit := append(searchKey, ClusteringType)
				lowerLimit = append(lowerLimit, clusteringKyes[index].getType().getVal())
				upperLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				iterTemp := storedb.NewIterator(nil, nil)
				iterTemp.Seek(upperLimit)
				isAtFirst = true
				for (isAtFirst || iterTemp.Next()) && bytes.HasPrefix(iterTemp.Key(), upperLimit){
						isAtFirst = false
				}
				iter = storedb.NewIterator(&util.Range{Start: lowerLimit, Limit: iterTemp.Key()}, nil)
				isAtFirst = false
				
				case BETWEEN:
				lowerLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				index++
				upperLimit := append(searchKey,buildKey(ClusteringType, clusteringKyes[index])...)
				iter = storedb.NewIterator(&util.Range{Start: lowerLimit , Limit: upperLimit}, nil)
			}
		}
		i := 0
		for isAtFirst || iter.Next() {
			isAtFirst = false
			if iter.Key() == nil{
				break
			}
			cKeyvalues := []FleetDBValue{}
			colvalues := []FleetDBValue{}
			//fmt.Printf("key: %s | value: %s\n", item.Key, item.Value)
			r := bytes.NewReader(iter.Key())
				p := make([]byte, 1)
				_ , err := r.Read(p)
				j := uint8(1)
				for err != io.EOF{
					var val FleetDBValue
					keyType := ColKeyType(p[0])
					r.Read(p)
					dataType := FleetDBType(p[0])
					switch dataType {
						case Int:
							val = NewIntValue(r)
						case BigInt:
							val = NewBigIntValue(r)
						case Float:
							val = NewFloatValue(r)
						case Double:
							val = NewDoubleValue(r)			
						case Text:
							val = NewTextValueFromNullTerminatedStream(r)
						case Boolean:
							val = NewBooleanValue(r)
					}
					if(keyType == PRIMARY){
						valMap[seqMap[j]] = val
					}
					if(keyType == CLUSTERING){
						valMap[seqMap[j]] = val
						cKeyvalues = append(cKeyvalues, val)
					}else if(keyType == COLUMN) {
						colvalues = append(colvalues,NewTextValue(bytes.NewReader(iter.Value())))
						valMap[val.String()] = NewTextValue(bytes.NewReader(iter.Value()))
					}
					_ , err = r.Read(p)
					j += 1
 				}
				isEq := equal(oldCKeyvalues, cKeyvalues)
				if i != 0 && (!isEq){
					myrow := createRowFromMap(oldValMap, columns)
					rows = append(rows,myrow)
					oldColvalues = colvalues
					oldValMap = valMap
					
				}else{
					oldColvalues = append(oldColvalues,colvalues...)
					for k,v := range valMap {
					  oldValMap[k] = v
					}
				}
				oldCKeyvalues = cKeyvalues
				valMap = make(map[string]FleetDBValue)
				//oldValMap = valMap
				//valMap := make(map[string]FleetDBValue)
				//oldValMap := make(map[string]FleetDBValue)
				i++
		}
		myrow := createRowFromMap(oldValMap, columns)
		rows = append(rows,myrow)
	return rows,err
}

func createRowFromMap(valMap map[string]FleetDBValue, columns []string) Row{
	row := []FleetDBValue{}
	for _ , colName := range columns{
		row = append(row, valMap[colName])
	}
	return Row{row} 
}

func equal(val1 []FleetDBValue, val2 []FleetDBValue) bool{
	if(len(val1) != len(val2)){
		return false
	}
	for i, _ := range val1{
		if val1[i].String() != val2[i].String(){
			return false
		} 
	}
	return true
}