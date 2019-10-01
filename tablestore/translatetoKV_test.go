package tablestore

import (
    "testing"
    "fmt"
)

func TestTranslateToKV(t *testing.T) {

	
	//Test for Valid Arguments
	createCommand := "CREATE TABLE crossfit_gyms (country_code text,state_province text,city text,gym_name text,PRIMARY KEY (country_code, state_province));"
	fmt.Println(createCommand)
	tableMap := make(map[string][]FleetDbColumnSpec)
	myschema,tableName := createSchema(createCommand)
	tableMap[tableName] = myschema
	insertCommand := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand)
	TranslateToKV(tableMap[myTableName], rowData)
	

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