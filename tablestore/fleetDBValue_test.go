package tablestore

import (
    "testing"
   // "fmt"
    "bytes"
    "github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	myInt := IntValue{200000}
	byteSlice := myInt.Serialize()
	//fmt.Println(byteSlice)
	reader := bytes.NewReader(byteSlice)
	assert.Equal(t, int32(200000), NewIntValue(reader).val)
	
	myFloat := FloatValue{3.14444}
	byteSlice = myFloat.Serialize()
	//fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	assert.Equal(t, float32(3.14444), NewFloatValue(reader).val)

	myLong := BigIntValue{20000000000}
	byteSlice = myLong.Serialize()
	//fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	assert.Equal(t, int64(20000000000), NewBigIntValue(reader).val)
	
	myDouble := DoubleValue{3.14444454543546}
	byteSlice = myDouble.Serialize()
	//fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	assert.Equal(t, float64(3.14444454543546), NewDoubleValue(reader).val)
	
	myString := TextValue{"Hello World"}
	byteSlice = myString.Serialize()
	reader = bytes.NewReader(byteSlice)
	assert.Equal(t, "Hello World", NewTextValue(reader).val)
	
	myNullTerminatedString := TextValue{"Hello World\000"}
	byteSlice = myNullTerminatedString.Serialize()
	reader = bytes.NewReader(byteSlice)
	//fmt.Println(NewTextValueFromNullTerminatedStream(reader).val)
	assert.Equal(t, "Hello World", NewTextValueFromNullTerminatedStream(reader).val)
	
	myBoolean := BooleanValue{true}
	byteSlice = myBoolean.Serialize()
	//fmt.Println(byteSlice)
	reader = bytes.NewReader(byteSlice)
	assert.Equal(t, true, NewBooleanValue(reader).val)
}

