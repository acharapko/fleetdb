package tablestore

import (
    "testing"
    "fmt"
)

func TestSerialize(t *testing.T) {
	myInt := Int{200000}
	byteSlice := myInt.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	newInt := myInt.Deserialize(byteSlice, 0)
	fmt.Println((*newInt))
	
	myFloat := Float{3.14444}
	byteSlice = myFloat.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	newFloat := myFloat.Deserialize(byteSlice, 0)
	fmt.Println((*newFloat))
	
	myLong := Long{20000000000}
	byteSlice = myLong.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	newLong := myLong.Deserialize(byteSlice, 0)
	fmt.Println((*newLong))
	
	myDouble := Double{3.14444454543546}
	byteSlice = myDouble.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	newDouble := myDouble.Deserialize(byteSlice, 0)
	fmt.Println((*newDouble))
	
	myString := String{"Hello World"}
	byteSlice = myString.Serialize()
	fmt.Println(byteSlice)
	//mySlice := []byte{64,14,3,45}
	newString := myString.Deserialize(byteSlice, 0)
	fmt.Println((*newString))
}

