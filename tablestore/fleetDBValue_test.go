package tablestore

import (
    "testing"
    "fmt"
    "bytes"
)

func TestSerialize(t *testing.T) {
	myInt := IntValue{200000}
	byteSlice := myInt.Serialize()
	fmt.Println(byteSlice)
	reader := bytes.NewReader(byteSlice)
	myInt.Deserialize(reader)
	if myInt.val != 200000 {
		t.Errorf("Int Serialization DeSerialization failed, expected %v, got %v", "200000" , myInt.val)
	}
	
	myFloat := FloatValue{3.14444}
	byteSlice = myFloat.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myFloat.Deserialize(reader)
	if myFloat.val != 3.14444 {
		t.Errorf("Float Serialization DeSerialization failed, expected %v, got %v", "3.14444" , myFloat.val)
	}
	
	myLong := BigIntValue{20000000000}
	byteSlice = myLong.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myLong.Deserialize(reader)
	if myLong.val != 20000000000 {
		t.Errorf("Long Serialization DeSerialization failed, expected %v, got %v", "20000000000" , myLong.val)
	}
	
	myDouble := DoubleValue{3.14444454543546}
	byteSlice = myDouble.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myDouble.Deserialize(reader)
	if myDouble.val != 3.14444454543546 {
		t.Errorf("Double Serialization DeSerialization failed, expected %v, got %v", "3.14444454543546" , myDouble.val)
	}
	
	myString := TextValue{"Hello World"}
	byteSlice = myString.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myString.Deserialize(reader)
	if myString.val != "Hello World" {
		t.Errorf("String Serialization DeSerialization failed, expected %v, got %v", "Hello World" , myString.val)
	}
	
	myBoolean := BooleanValue{true}
	byteSlice = myBoolean.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myBoolean.Deserialize(reader)
	if myBoolean.val != true {
		t.Errorf("Boolean Serialization DeSerialization failed, expected %v, got %v", "true" , myBoolean.val)
	}
}

