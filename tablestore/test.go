package main

import (
	"fmt"
	"strings"
	"github.com/acharapko/fleetdb/kv_store"
	//"github.com/acharapko/fleetdb/ids"
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

type KVItem struct {
    Key      []byte
    Value    []byte
}

type FleetDbColumnSpec struct {
    colname         string
    coltype         FleetDBType
    isPartition     bool
    isClustering    bool
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
	fmt.Println("before new store")
	kv_store.NewStore()
	fmt.Println("After new store")
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

func decodeInsertCommand(query string)([][]string , string){
	val := [][]string{{"country_code", "US"},{"state_province", "NY"},{"city", "Buffalo"},{"gym_name", "University Avenue"}}
	return val, "crossfit_gyms"
}

func TranslateToKV(columnSpecs []FleetDbColumnSpec, values [][]string) []KVItem{
    //build common string
    var sbPKey strings.Builder
    var sbCKey strings.Builder
    for i, colSpec := range columnSpecs {
    	if colSpec.isPartition{
    		fmt.Println(values[i][1])
    		sbPKey.WriteString(values[i][1])
    	}
    	if colSpec.isClustering{
    		fmt.Println(values[i][1])
    		sbCKey.WriteString(values[i][1])
    	}
   }
    kvItems := make([]KVItem, 2)
    index := 0
    prefix := sbPKey.String() + "/"+ sbCKey.String()
    for i, colSpec := range columnSpecs {
    	if !colSpec.isPartition && !colSpec.isClustering{
    		key := prefix + "/" + values[i][0]
    		val := values[i][1]
    		fmt.Println(key + " = " + val)
    		kvItems[index] =  KVItem{[]byte(key), []byte(val)}
    		index = index + 1
    	}
   }
	//sbPKey.WriteString(str)
	return kvItems;
}
