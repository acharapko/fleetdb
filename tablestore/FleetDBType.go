package tablestore

import (
	"encoding/binary"
	"fmt"
	"bytes"
)
type FleetDBType interface{
	 Serialize() []byte // this translates to []bytes
	 Deserialize([]byte, int) *FleetDBType //new type
	 //FromString(string) *FleetDBType //new from string
}
type Int struct{
	val int32
}

type Float struct{
	val float32
}

type Long struct{
	val int64
}

type Double struct{
	val float64
}

type String struct{
	val string
}

func (i Int) String() string {
	return fmt.Sprintf("Integer Value = %v", i.val)
}

func (i Int) Serialize() []byte{
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, i.val)
    if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
    return buf.Bytes()
}

func (i Int) Deserialize(values []byte, offset int) *FleetDBType{
	var myInt FleetDBType
	var data int32
	buf := bytes.NewReader(values)
	err := binary.Read(buf, binary.LittleEndian, &data)
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	myInt = Int{data}
	return &myInt
}

func (f Float) String() string{
	return fmt.Sprintf("Float value = %v", f.val)
}

func (f Float) Serialize() []byte{
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, f.val)
	if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
	return buf.Bytes()
}

func (f Float) Deserialize(values []byte, offset int) *FleetDBType {
	var myFloat FleetDBType 
	var data float32
	buf := bytes.NewReader(values)
	err := binary.Read(buf, binary.LittleEndian, &data)
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
	myFloat = Float{data}
	return &myFloat
	
}

func (l Long) String() string {
	return fmt.Sprintf("Long Value = %v", l.val)
}

func (l Long) Serialize() []byte{
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, l.val)
    if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
    return buf.Bytes()
}

func (l Long) Deserialize(values []byte, offset int) *FleetDBType{
	var myLong FleetDBType
	var data int64
	buf := bytes.NewReader(values)
	err := binary.Read(buf, binary.LittleEndian, &data)
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	myLong = Long{data}
	return &myLong
}

func (d Double) String() string{
	return fmt.Sprintf("Double value = %v", d.val)
}

func (d Double) Serialize() []byte{
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, d.val)
	if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
	return buf.Bytes()
}

func (d Double) Deserialize(values []byte, offset int) *FleetDBType {
	var myDouble FleetDBType 
	var data float64
	buf := bytes.NewReader(values)
	err := binary.Read(buf, binary.LittleEndian, &data)
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
	myDouble = Double{data}
	return &myDouble
	
}

func (s String) String() string{
	return fmt.Sprintf("String value = %v", s.val)
}

func (s String) Serialize() []byte{
	return []byte(s.val)
}

func (s String) Deserialize(values []byte, offset int) *FleetDBType {
	var myString FleetDBType 
	var data string
	data = string(values)
	myString = String{data}
	return &myString
	
}
