package tablestore

import (
    "testing"
    "github.com/stretchr/testify/assert"
    //"fmt"
)

func TestTranslateToKV(t *testing.T) {
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
	insertCommand := "INSERT INTO crossfit_gyms (country_code, state_province, city, gym_name) VALUES ('US', ‘NY’, ‘Buffalo’, 'University Avenue');";
	rowData, myTableName := decodeInsertCommand(insertCommand)
	res := TranslateToKV(tableMap[myTableName], rowData)
	
	expectedByteKey := [][][]byte{{[]byte{1},[]byte{5},[]byte("US\000"), []byte{2},[]byte{5},[]byte("NY\000"), []byte{3},[]byte{5},[]byte("city\000")},
	{[]byte{1},[]byte{5},[]byte("US\000"), []byte{2},[]byte{5},[]byte("NY\000"), []byte{3},[]byte{5},[]byte("gym_name\000")}}
	
	expectedSingeDKey := [][]byte{}
	for i, _:= range expectedByteKey{
		oneKey := make([]byte,0) 
		for j, _:= range expectedByteKey[i]{
			oneKey = append(oneKey, expectedByteKey[i][j]...)
		}
		expectedSingeDKey = append(expectedSingeDKey, oneKey)
	}
	expectedVal := []string{"Buffalo","University Avenue"}
	for i ,_ := range res {
		assert.Equal(t, expectedSingeDKey[i], res[i].Key)
		assert.Equal(t, expectedVal[i], string(res[i].Value))
	}
	
	
	//Test With One Partition Key and Zero Clustering Key
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, false, 2}
	res = TranslateToKV(tableMap[myTableName], rowData)
	
	expectedByteKey = [][][]byte{{[]byte{1},[]byte{5},[]byte("US\000"), []byte{3},[]byte{5},[]byte("state_province\000")},
		{[]byte{1},[]byte{5},[]byte("US\000"), []byte{3},[]byte{5},[]byte("city\000")},
	{[]byte{1},[]byte{5},[]byte("US\000"), []byte{3},[]byte{5},[]byte("gym_name\000")}}
	
	expectedSingeDKey = [][]byte{}
	for i, _:= range expectedByteKey{
		oneKey := make([]byte,0) 
		for j, _:= range expectedByteKey[i]{
			oneKey = append(oneKey, expectedByteKey[i][j]...)
		}
		expectedSingeDKey = append(expectedSingeDKey, oneKey)
	}
	expectedVal = []string{"NY", "Buffalo","University Avenue"}
	for i ,_ := range res {
		assert.Equal(t, expectedSingeDKey[i], res[i].Key)
		assert.Equal(t, expectedVal[i], string(res[i].Value))
	}
	
	
	//All columns are either PK or CK
	myschema[1] = FleetDbColumnSpec{"state_province", myText, false, true, 2}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, true, 3}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, true, 4}
	res = TranslateToKV(tableMap[myTableName], rowData)
	
	expectedByteKey = [][][]byte{{[]byte{1},[]byte{5},[]byte("US\000"), []byte{2},[]byte{5},[]byte("NY\000"), []byte{2},[]byte{5},[]byte("Buffalo\000"), []byte{2},[]byte{5},[]byte("University Avenue\000")}}
	
	expectedSingeDKey = [][]byte{}
	for i, _:= range expectedByteKey{
		oneKey := make([]byte,0) 
		for j, _:= range expectedByteKey[i]{
			oneKey = append(oneKey, expectedByteKey[i][j]...)
		}
		expectedSingeDKey = append(expectedSingeDKey, oneKey)
	}
	for i ,_ := range res {
		assert.Equal(t, expectedSingeDKey[i], res[i].Key)
		assert.Equal(t,[]byte(nil), res[i].Value)
	}
	
	// Composite Key
	myschema[1] = FleetDbColumnSpec{"state_province", myText, true, false, 2}
	myschema[2] = FleetDbColumnSpec{"city", myText, false, false, 3}
	myschema[3] = FleetDbColumnSpec{"gym_name", myText, false, false, 4}
	res = TranslateToKV(tableMap[myTableName], rowData)
	
	expectedByteKey = [][][]byte{{[]byte{1},[]byte{5},[]byte("US\000"), []byte{1},[]byte{5},[]byte("NY\000"), []byte{3},[]byte{5},[]byte("city\000")},
	{[]byte{1},[]byte{5},[]byte("US\000"), []byte{1},[]byte{5},[]byte("NY\000"), []byte{3},[]byte{5},[]byte("gym_name\000")}}
	
	expectedSingeDKey = [][]byte{}
	for i, _:= range expectedByteKey{
		oneKey := make([]byte,0) 
		for j, _:= range expectedByteKey[i]{
			oneKey = append(oneKey, expectedByteKey[i][j]...)
		}
		expectedSingeDKey = append(expectedSingeDKey, oneKey)
	}
	expectedVal = []string{"Buffalo","University Avenue"}
	for i ,_ := range res {
		assert.Equal(t, expectedSingeDKey[i], res[i].Key)
		assert.Equal(t, expectedVal[i], string(res[i].Value))
	}
}

func decodeInsertCommand(query string)([]FleetDBValue , string){
	val := []FleetDBValue{TextValue{"US"}, TextValue{"NY"},TextValue{"Buffalo"},TextValue{"University Avenue"}}
	return val, "crossfit_gyms"
}
