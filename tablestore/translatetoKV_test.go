package tablestore

import (
    "testing"
   // "github.com/stretchr/testify/assert"
    //"fmt"
)

func TestTranslateToKV(t *testing.T) {
	//createCommand := "CREATE TABLE crossfit_gyms (country_code string,state_province string,city string,gym_name string,PRIMARY KEY (country_code, state_province));"
	tableMap := make(map[string][]FleetDbColumnSpec)
	tableName := "crossfit_gyms"
	
	var myText TextValue
	
	
	//Test With One Partition Key and One Clustering Key
	myschema := make([]FleetDbColumnSpec, 4)
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, false}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false}
	tableMap[tableName] = myschema
	insertCommand := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand)
	res := TranslateToKV(tableMap[myTableName], rowData)
	parseKey(res)
	//fmt.Println(res[0].Key)
	//fmt.Println(res[1].Key)
	
	//fmt.Println(string([]byte{3,5,99,105,116,121,0}))
	//assert.Equal(t, "US/NY/city", string(res[0].Key))
	//assert.Equal(t, "Buffalo", string(res[0].Value))
	//assert.Equal(t, "US/NY/gym_name", string(res[1].Key))
	//assert.Equal(t, "University Avenue", string(res[1].Value))
	/*
	//Test With One Partition Key and Zero Clustering Key
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, false}
	res = TranslateToKV(tableMap[myTableName], rowData)
	assert.Equal(t, "US/state_province", string(res[0].Key))
	assert.Equal(t, "NY", string(res[0].Value))
	assert.Equal(t, "US/city", string(res[1].Key))
	assert.Equal(t, "Buffalo", string(res[1].Value))
	assert.Equal(t, "US/gym_name", string(res[2].Key))
	assert.Equal(t, "University Avenue", string(res[2].Value))
	
	//All columns are either PK or CK
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, true}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, true}
	res = TranslateToKV(tableMap[myTableName], rowData)
	assert.Equal(t, "US/NY/Buffalo/University Avenue", string(res[0].Key))
	assert.Equal(t,[]byte(nil), res[0].Value)
	
	// Composite Key
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, true, false}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, false}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false}
	res = TranslateToKV(tableMap[myTableName], rowData)
	assert.Equal(t, "US/NY/city", string(res[0].Key))
	assert.Equal(t, "Buffalo", string(res[0].Value))
	assert.Equal(t, "US/NY/gym_name", string(res[1].Key))
	assert.Equal(t, "University Avenue", string(res[1].Value))
	
	//Composite Key with Clustering Key
	myschema[0] = FleetDbColumnSpec{"country_code", myText, true, false}
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true}
	myschema[2] = FleetDbColumnSpec{"city", myText, true, false}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false}
	res = TranslateToKV(tableMap[myTableName], rowData)
	assert.Equal(t, "US/Buffalo/NY/gym_name", string(res[0].Key))
	assert.Equal(t, "University Avenue", string(res[0].Value))
	*/
}

func decodeInsertCommand(query string)([]FleetDBValue , string){
	val := []FleetDBValue{TextValue{"US"}, TextValue{"NY"},TextValue{"Buffalo"},TextValue{"University Avenue"}}
	return val, "crossfit_gyms"
}


/*func createSchema(query string) ([]FleetDbColumnSpec,string) {
	schema := make([]FleetDbColumnSpec, 4)
	schema[0] = FleetDbColumnSpec{"country_code", string, true, false}
	schema[1] = FleetDbColumnSpec{"state_province", string, false, true}
	schema[2] = FleetDbColumnSpec{"city", string, false, false}
	schema[3] = FleetDbColumnSpec{"gym_name", string, false, false}
	return schema,"crossfit_gyms"
}
*/
