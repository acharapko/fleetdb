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
	//mySlice := []byte{64,14,3,45}
	reader := bytes.NewReader(byteSlice)
	myInt.Deserialize(reader)
	fmt.Println(myInt.val)
	
	myFloat := FloatValue{3.14444}
	byteSlice = myFloat.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	myFloat.Deserialize(reader)
	fmt.Println(myFloat.val)
	
	myLong := BigIntValue{20000000000}
	byteSlice = myLong.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	reader = bytes.NewReader(byteSlice)
	myLong.Deserialize(reader)
	fmt.Println(myLong.val)
	
	myDouble := DoubleValue{3.14444454543546}
	byteSlice = myDouble.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	reader = bytes.NewReader(byteSlice)
	myDouble.Deserialize(reader)
	fmt.Println(myDouble.val)
	
	myString := TextValue{"Hello World"}
	byteSlice = myString.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	//mySlice := []byte{64,14,3,45}
	myString.Deserialize(reader)
	fmt.Println(myString.val)
	
	myBoolean := BooleanValue{true}
	byteSlice = myBoolean.Serialize()
	fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	//mySlice := []byte{64,14,3,45}
	myBoolean.Deserialize(reader)
	fmt.Println(myBoolean.val)
}

