package main

import (
	"fmt"
	//"strings"
	//"github.com/darshannevgi/fleetdb/kv_store"
	//"github.com/darshannevgi/fleetdb/tablestore"
)
/*
CREATE TABLE crossfit_gyms (  
   country_code text,  
   state_province text,  
   city text,  
   gym_name text,  
   PRIMARY KEY (country_code, state_province)  
);
*/

type FleetDBType uint8

const (
  INT FleetDBType = iota
  FLOAT
  TEXT
)

type FleetDbColumnSpec struct {
    colname         string
    coltype         FleetDBType
    isPartition     bool
    isClustering    bool
}

type KVItem struct {
    Key      []byte
    Value    []byte
}

//m := make(map[string]int)
func main(){
	createCommand := "CREATE TABLE crossfit_gyms (country_code text,state_province text,city text,gym_name text,PRIMARY KEY (country_code, state_province));"
	fmt.Println(createCommand)
	tableMap := make(map[string][]FleetDbColumnSpec)
	myschema,tableName := createSchema(createCommand)
	tableMap[tableName] = myschema
	insertCommand := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand)
	//kvData := TranslateToKV(tableMap[myTableName], rowData)
	TranslateToKV(tableMap[myTableName], rowData)
	//fmt.Println("before new store")
	//kv_store.NewStore()
	//fmt.Println("After new store")
	/*for _, item := range kvItem{
		cmd := kv_store.Command{tableName, item.Key, item.Value, "", 0, kv_store.PUT }
		//var id = flag.String("id", "1.1", "ID in format of Zone.Node. Default 1.1")
		id := ids.NewID(5,5)
		_, err := store.Execute(cmd, id)
		if err != nil {
			fmt.Println("Put Sucessful")
		}else {
			fmt.Println("Put Unsucessful")
		}		
	}
	*/
	
//	success := writeToDB(myTableName, ksvData)
//	if sucess {
//		fmt.Println("Write Successful")
//	}
//	returnData = readFromDB(myTableName)
//	isEqual := checkEqality(returnData, kvData)
//	if isEqual {
//		fmt.Println("Write and Read Happning correctly")
//	}
}

func createSchema(query string) ([]FleetDbColumnSpec,string) {
	schema := make([]FleetDbColumnSpec, 4)
	schema[0] = FleetDbColumnSpec{"country_code", TEXT, true, false}
	schema[1] = FleetDbColumnSpec{"state_province", TEXT, false, true}
	schema[2] = FleetDbColumnSpec{"city", TEXT, false, false}
	schema[3] = FleetDbColumnSpec{"gym_name", TEXT, false, false}
	return schema,"crossfit_gyms"
}

func decodeInsertCommand(query string)([][]byte , string){
	val := [][]byte{[]byte("US"), []byte("NY"),[]byte("Buffalo"),[]byte("University Avenue")}
	return val, "crossfit_gyms"
}
/* 
Each values[0] represents byte[] data value of first column
if first column is country_code then  values[0] is byte representation of 'US' = byte[]{85,83}
*/
func TranslateToKV(columnSpecs []FleetDbColumnSpec, values [][]byte) []KVItem{
    var pKey []byte
    var cKey []byte
    for i, colSpec := range columnSpecs {
    	if colSpec.isPartition{
    		pKey = append(pKey, values[i]...)
    	}
    	if colSpec.isClustering{
    		cKey = append(cKey, values[i]...)
    	}
   }
    kvItems := make([]KVItem, 100)
    index := 0
    pKey = append(pKey,"/"...)
    prefix := append(pKey,cKey...)
    prefix = append(prefix,"/"...)
    for i, colSpec := range columnSpecs {
    	if !colSpec.isPartition && !colSpec.isClustering{
    		key := append(prefix,colSpec.colname...)
    		val := values[i]
    		fmt.Println("Key is =" + string(key))
    		fmt.Println("Value is =" + string(val))
    		kvItems[index] =  KVItem{key, val}
    		index = index + 1
    	}
   }
	return kvItems;
}
