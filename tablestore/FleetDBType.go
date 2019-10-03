package tablestore

import (
	"encoding/binary"
	"fmt"
)
type FleetDBType interface{
	 Serialize() []byte // this translates to []bytes
	 Deserialize([]byte, int) *FleetDBType //new type
	 //FromString(string) *FleetDBType //new from string
}
type Int struct{
	val uint32
}

func (i Int) String() string {
	return fmt.Sprintf("Integer Value = %v", i.val)
}

func (i Int) Serialize() []byte{
	bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, i.val)
    return bs
    //fmt.Println(bs)
}

func (i Int) Deserialize(values []byte, offset int) *FleetDBType{
	var myInt FleetDBType
	data := binary.LittleEndian.Uint32(values)
	myInt = Int{data}
	return &myInt
    //fmt.Println(myInt)
}