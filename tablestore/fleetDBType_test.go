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
}

