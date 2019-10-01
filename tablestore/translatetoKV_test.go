package tablestore

import (
    "testing"
   // "fmt"
)

func TestTranslateToKV(t *testing.T) {

	
	
	//createCommand := "CREATE TABLE crossfit_gyms (country_code text,state_province text,city text,gym_name text,PRIMARY KEY (country_code, state_province));"
	//fmt.Println(createCommand)
	//myschema,tableName := createSchemaOnePOneC(createCommand)
	tableMap := make(map[string][]FleetDbColumnSpec)
	tableName := "crossfit_gyms"
	
	
	//Test With One Partition Key and One Clustering Key
	myschema := make([]FleetDbColumnSpec, 4)
	myschema[0] = FleetDbColumnSpec{"country_code", TEXT, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", TEXT, false, true}
	myschema[2] = FleetDbColumnSpec{"city", TEXT, false, false}
	myschema[3] = FleetDbColumnSpec{"gym_name", TEXT, false, false}
	
	tableMap[tableName] = myschema
	insertCommand := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand)
	res := TranslateToKV(tableMap[myTableName], rowData)
	if string(res[0].Key) != "US/NY/city" {
		t.Errorf("TranslateToKV() failed, expected %v, got %v", "US/NY/city" , string(res[0].Key))
	}
	if string(res[0].Value) != "Buffalo" {
		t.Errorf("TranslateToKV() failed, expected %v, got %v", "Buffalo" , string(res[0].Value))
	}
	if string(res[1].Key) != "US/NY/gym_name" {
		t.Errorf("TranslateToKV() failed, expected %v, got %v", "US/NY/gym_name" , string(res[1].Key))
	}
	if string(res[1].Value) != "University Avenue" {
		t.Errorf("TranslateToKV() failed, expected %v, got %v", "University Avenue" , string(res[1].Value))
	}
	//fmt.Println(string(res[0].Key));
	
	
	//Test With One Partition Key and Zero Clustering Key
	myschema = make([]FleetDbColumnSpec, 4)
	myschema[0] = FleetDbColumnSpec{"country_code", TEXT, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", TEXT, false, false}
	myschema[2] = FleetDbColumnSpec{"city", TEXT, false, false}
	myschema[3] = FleetDbColumnSpec{"gym_name", TEXT, false, false}
	
	tableMap[tableName] = myschema
	insertCommand = "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName = decodeInsertCommand(insertCommand)
	res = TranslateToKV(tableMap[myTableName], rowData)
	if string(res[0].Key) != "US/state_province" {
		t.Errorf("TranslateToKV() Test case 2 failed, expected %v, got %v", "US/state_province" , string(res[0].Key))
	}
	if string(res[0].Value) != "NY" {
		t.Errorf("TranslateToKV() Test case 2  failed, expected %v, got %v", "NY" , string(res[0].Value))
	}
	if string(res[1].Key) != "US/city" {
		t.Errorf("TranslateToKV() Test case 2 failed, expected %v, got %v", "US/city" , string(res[1].Key))
	}
	if string(res[1].Value) != "Buffalo" {
		t.Errorf("TranslateToKV() Test case 2  failed, expected %v, got %v", "Buffalo" , string(res[1].Value))
	}
	if string(res[2].Key) != "US/gym_name" {
		t.Errorf("TranslateToKV() Test case 2 failed, expected %v, got %v", "US/gym_name" , string(res[2].Key))
	}
	if string(res[2].Value) != "University Avenue" {
		t.Errorf("TranslateToKV() Test case 2 failed, expected %v, got %v", "University Avenue" , string(res[2].Value))
	}
	

}

func decodeInsertCommand(query string)([][]byte , string){
	val := [][]byte{[]byte("US"), []byte("NY"),[]byte("Buffalo"),[]byte("University Avenue")}
	return val, "crossfit_gyms"
}

/*func createSchema(query string) ([]FleetDbColumnSpec,string) {
	schema := make([]FleetDbColumnSpec, 4)
	schema[0] = FleetDbColumnSpec{"country_code", TEXT, true, false}
	schema[1] = FleetDbColumnSpec{"state_province", TEXT, false, true}
	schema[2] = FleetDbColumnSpec{"city", TEXT, false, false}
	schema[3] = FleetDbColumnSpec{"gym_name", TEXT, false, false}
	return schema,"crossfit_gyms"
}
*/
