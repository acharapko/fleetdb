package tablestore

import (
    "testing"
    "github.com/google/uuid"
   // "fmt"
    "github.com/stretchr/testify/assert"
)

func createSampleDB()(uuid.UUID, []FleetDBValue, []string, FleetDBTableSpec, Store){
	db := NewStore() 
	tableid := uuid.New()
	tableMap := make(map[string][]FleetDbColumnSpec)
	tableName := "crossfit_gyms"
	var myText TextValue
	
	//Test With One Partition Key and One Clustering Key
	myschema := make([]FleetDbColumnSpec, 4)
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false, 1}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true, 2}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, false, 3}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false, 4}
	tableMap[tableName] = myschema
	
	insertCommand1 := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand1)
	res := TranslateToKV(tableMap[myTableName], rowData)
	data := TranslateToKV(tableMap[tableName], []FleetDBValue{TextValue{"US"}, TextValue{"Ohio"},TextValue{"Columbus"},TextValue{"Englewood Avenue"}})
	res = append(res,data...)
	db.Save(tableid, res)
	
	columnSpecs := make(map[string]*FleetDbColumnSpec)
	columnSpecs["country_code"] = &myschema[0] 
	columnSpecs["state_province"] = &myschema[1] 
	columnSpecs["city"] = &myschema[2]
	columnSpecs["gym_name"] = &myschema[3] 
	tableSpec := FleetDBTableSpec{columnSpecs}
	partitionKeys := []FleetDBValue{TextValue{"US"}}
	columns := []string{"country_code","state_province", "city", "gym_name"}
	return tableid,partitionKeys,columns,tableSpec,db
}

func TestBETWEEN(t *testing.T) {
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	//fmt.Println("Testing BETWEEN")
	expectedVal := [][]string{{"US", "NY", "Buffalo", "University Avenue"}}
	clusteringKyes := []FleetDBValue {TextValue{"ABC"}, TextValue{"OA"}}
	clusteringComparators := []ComparatorType{BETWEEN}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}	

func TestGREATERTHANEQUAL(t *testing.T) {	
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	//fmt.Println("Testing GREATERTHANEQUAL 1")
	expectedVal := [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringKyes := []FleetDBValue {TextValue{"NY"}}
	clusteringComparators := []ComparatorType{GREATERTHANEQUAL}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	//fmt.Println("Testing GREATERTHANEQUAL 2")
	expectedVal = [][]string{{"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringKyes = []FleetDBValue {TextValue{"NYA"}}
	clusteringComparators = []ComparatorType{GREATERTHANEQUAL}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 1)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}

func TestGREATERTHAN(t *testing.T) {	
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	//fmt.Println("Testing GREATERTHAN 1")
	expectedVal := [][]string{{"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringKyes := []FleetDBValue {TextValue{"NY"}}
	clusteringComparators := []ComparatorType{GREATERTHAN}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	assert.Equal(t, len(rows), 1)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	//fmt.Println("Testing GREATERTHAN 2")
	expectedVal = [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringKyes = []FleetDBValue {TextValue{"ABC"}}
	clusteringComparators = []ComparatorType{GREATERTHAN}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}

func TestLESSERTHANEQUAL(t *testing.T) {	
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	//fmt.Println("Testing LESSERTHANEQUAL 1")
	clusteringKyes := []FleetDBValue {TextValue{"Ohio"}}
	expectedVal := [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringComparators := []ComparatorType{LESSTHANEQUAL}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	//fmt.Println(len(rows))
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	//fmt.Println("Testing LESSERTHANEQUAL 2")
	clusteringKyes = []FleetDBValue {TextValue{"NY"}}
	expectedVal = [][]string{{"US", "NY", "Buffalo", "University Avenue"}}
	clusteringComparators = []ComparatorType{LESSTHANEQUAL}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 1)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}	

func TestLESSERTHAN(t *testing.T) {
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	//fmt.Println("Testing LESSERTHAN 1")
	clusteringKyes := []FleetDBValue {TextValue{"Ohio"}}
	expectedVal := [][]string{{"US", "NY", "Buffalo", "University Avenue"}}
	clusteringComparators := []ComparatorType{LESSTHAN}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	assert.Equal(t, len(rows), 1)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	//fmt.Println("Testing LESSERTHAN 2")
	clusteringKyes = []FleetDBValue {TextValue{"ZAB"}}
	expectedVal = [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	clusteringComparators = []ComparatorType{LESSTHAN}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}

func TestCreateRowFromMap(t *testing.T) {
	columns := []string{"country_code","state_province", "city", "gym_name"}
	valMap := make(map[string]FleetDBValue)
	valMap["country_code"] = TextValue{"US"}
	valMap["state_province"] = TextValue{"NY"}
	valMap["city"] = TextValue{"Buffalo"}
	valMap["gym_name"] = TextValue{"University Avenue"}
	
	//All rows required
	row := createRowFromMap(valMap, columns)
	assert.Equal(t, row.Values[0], TextValue{"US"})
	assert.Equal(t, row.Values[1], TextValue{"NY"})
	assert.Equal(t, row.Values[2], TextValue{"Buffalo"})
	assert.Equal(t, row.Values[3], TextValue{"University Avenue"})
	
	//Only 2 row required
	columns = []string{"country_code", "gym_name"}
	row = createRowFromMap(valMap, columns)
	assert.Equal(t, row.Values[0], TextValue{"US"})
	assert.Equal(t, row.Values[1], TextValue{"University Avenue"})
	
	//when column value is not present in database
	columns = []string{"country_code", "street_name"}
	row = createRowFromMap(valMap, columns)
	assert.Equal(t, row.Values[0], TextValue{"US"})
	assert.Equal(t, row.Values[1], nil)
	
}

func TestGetPartition(t *testing.T) {
	tableid,partitionKeys,columns,tableSpec,db := createSampleDB()
	
	//Retrieving only 2 columns
	columns = []string{"state_province", "city"}
	clusteringKyes := []FleetDBValue {}
	clusteringComparators := []ComparatorType{}
	expectedVal := [][]string{{"NY", "Buffalo"}, {"Ohio", "Columbus"}}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	
	//Retrieving All columns
	columns = []string{"country_code", "state_province", "city", "gym_name"}
	clusteringKyes = []FleetDBValue {}
	clusteringComparators = []ComparatorType{}
	expectedVal = [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
}

func TestSave(t *testing.T){
	
	//Test Insert	
	db := NewStore() 
	tableid := uuid.New()
	tableMap := make(map[string][]FleetDbColumnSpec)
	tableName := "crossfit_gyms"
	var myText TextValue
	
	myschema := make([]FleetDbColumnSpec, 4)
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false, 1}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true, 2}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, false, 3}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false, 4}
	tableMap[tableName] = myschema
	
	insertCommand1 := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand1)
	res := TranslateToKV(tableMap[myTableName], rowData)
	data := TranslateToKV(tableMap[tableName], []FleetDBValue{TextValue{"US"}, TextValue{"Ohio"},TextValue{"Columbus"},TextValue{"Englewood Avenue"}})
	res = append(res,data...)
	db.Save(tableid, res)
	
	
	columnSpecs := make(map[string]*FleetDbColumnSpec)
	columnSpecs["country_code"] = &myschema[0] 
	columnSpecs["state_province"] = &myschema[1] 
	columnSpecs["city"] = &myschema[2]
	columnSpecs["gym_name"] = &myschema[3] 
	tableSpec := FleetDBTableSpec{columnSpecs}
	partitionKeys := []FleetDBValue{TextValue{"US"}}
	columns := []string{"country_code", "state_province", "city", "gym_name"}
	clusteringKyes := []FleetDBValue {}
	clusteringComparators := []ComparatorType{}
	expectedVal := [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood Avenue"}}
	rows, _ := db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum := 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}
	
	// Test Update
	
	data = TranslateToKV(tableMap[tableName], []FleetDBValue{TextValue{"US"}, TextValue{"Ohio"},TextValue{"Columbus"},TextValue{"Englewood New Avenue"}})
	db.Save(tableid, data)
	
	expectedVal = [][]string{{"US", "NY", "Buffalo", "University Avenue"}, {"US", "Ohio", "Columbus", "Englewood New Avenue"}}
	rows, _ = db.GetPartition(tableid, partitionKeys, clusteringKyes, clusteringComparators, columns, tableSpec)
	rowNum = 0
	assert.Equal(t, len(rows), 2)
	for _, row := range rows{
		//ch := ""
		index := 0
		for _, val := range row.Values{
			assert.Equal(t, val.String(), expectedVal[rowNum][index])
			if(val !=  nil){
				//fmt.Printf(ch + val.String())
			}
			//ch = " || "
			index += 1
		}
		//fmt.Println()
		rowNum++
	}

}
		
