package tablestore

import (
	"io"
	"encoding/binary"
	"fmt"
	"bytes"
)
type FleetDBValue interface{
	 Serialize() []byte // this translates to []bytes
	 Deserialize(io.Reader) //new type
	 //FromString(string) *FleetDBValue //new from string
}
type IntValue struct{
	val int32
}

type FloatValue struct{
	val float32
}

type BigIntValue struct{
	val int64
}

type DoubleValue struct{
	val float64
}

type TextValue struct{
	val string
}

type BooleanValue struct{
	val bool
}

func (i IntValue) String() string {
	return fmt.Sprintf("Integer Value = %v", i.val)
}

func (i IntValue) Serialize() []byte{
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, i.val)
    if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
    return buf.Bytes()
}

func (i IntValue) Deserialize(reader io.Reader){
	err := binary.Read(reader, binary.LittleEndian, &(i.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
}

func (f FloatValue) String() string{
	return fmt.Sprintf("FloatValue  = %v", f.val)
}

func (f FloatValue) Serialize() []byte{
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, f.val)
	if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
	return buf.Bytes()
}

func (f FloatValue) Deserialize(reader io.Reader){
	err := binary.Read(reader, binary.LittleEndian, &(f.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
}

func (l BigIntValue) String() string {
	return fmt.Sprintf("BigIntValue Value = %v", l.val)
}

func (l BigIntValue) Serialize() []byte{
    buf := new(bytes.Buffer)
    err := binary.Write(buf, binary.LittleEndian, l.val)
    if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
    return buf.Bytes()
}

func (l BigIntValue) Deserialize(reader io.Reader){
	err := binary.Read(reader, binary.LittleEndian, &(l.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
}

func (d DoubleValue) String() string{
	return fmt.Sprintf("DoubleValue = %v", d.val)
}

func (d DoubleValue) Serialize() []byte{
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, d.val)
	if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
	return buf.Bytes()
}

func (d DoubleValue) Deserialize(reader io.Reader){
	err := binary.Read(reader, binary.LittleEndian, &(d.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
}

func (s TextValue) String() string{
	return fmt.Sprintf("String value = %v", s.val)
}

func (s TextValue) Serialize() []byte{
	return []byte(s.val)
}

func (s TextValue) Deserialize(reader io.Reader){
	var value []byte
	err := binary.Read(reader, binary.LittleEndian, value)
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
	s.val = string(value)
}

func (b BooleanValue) String() string{
	return fmt.Sprintf("BooleanValue = %v", b.val)
}

func (b BooleanValue) Serialize() []byte{
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, b.val)
	if err != nil {
	    fmt.Println("binary.Write failed:", err)
    }
	return buf.Bytes()
}

func (b BooleanValue) Deserialize(reader io.Reader){
	err := binary.Read(reader, binary.LittleEndian, &(b.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
    }
}