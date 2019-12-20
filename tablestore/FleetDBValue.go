package tablestore

import (
	"io"
	"encoding/binary"
	"fmt"
	"bytes"
	"io/ioutil"
)
type FleetDBValue interface{
	 Serialize() []byte // this translates to []bytes
	 String() string
	 getType() FleetDBType
}

type FleetDBType uint8 
const (
    Int FleetDBType = iota + 1
    BigInt
    Float
    Double
    Text
    Boolean
)

type ComparatorType uint8
const (
	LESSTHAN = iota + 1
	LESSTHANEQUAL
	GREATERTHAN
	GREATERTHANEQUAL
	BETWEEN
	EQUAL
)

type ColKeyType uint8 
const (
    PRIMARY ColKeyType = iota + 1
    CLUSTERING
    COLUMN
)

func (f FleetDBType) getVal() uint8{
	return uint8(f)
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

func NewIntValue(reader io.Reader) *IntValue{
	var i IntValue
	err := binary.Read(reader, binary.LittleEndian, &(i.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	return &i
}

func (i IntValue) getType() FleetDBType{
	return Int;
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

func NewFloatValue(reader io.Reader) *FloatValue{
	var f FloatValue
	err := binary.Read(reader, binary.LittleEndian, &(f.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	return &f
}

func (f FloatValue) getType() FleetDBType{
	return Float
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

func NewBigIntValue(reader io.Reader) *BigIntValue{
	var l BigIntValue
	err := binary.Read(reader, binary.LittleEndian, &(l.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	return &l
}

func (l BigIntValue) getType() FleetDBType{
	return BigInt
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

func NewDoubleValue(reader io.Reader) *DoubleValue{
	var d DoubleValue
	err := binary.Read(reader, binary.LittleEndian, &(d.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	return &d
}

func (d DoubleValue) getType() FleetDBType{
	return Double
}

func (s TextValue) String() string{
	return fmt.Sprintf(s.val)
}

func (s TextValue) Serialize() []byte{
	return []byte(s.val)
}

func NewTextValue(reader io.Reader) *TextValue{
	b , err := ioutil.ReadAll(reader)
	if err != nil {
	    fmt.Println("readAll failed:", err)
	}
    ans := TextValue{string(b)}
    return &ans
}

func NewTextValueFromNullTerminatedStream(r io.Reader) *TextValue {
    p := []byte{}
    one := make([]byte, 1)
	_ , err := r.Read(one)
    for err != io.EOF && string(one) !=  "\000" {
    	p = append(p,one...)
    	_ , err = r.Read(one)
    }
    ans := TextValue{string(p)}
    return &ans
}

func (s TextValue) getType() FleetDBType{
	return Text
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

func NewBooleanValue(reader io.Reader) *BooleanValue{
	var b BooleanValue
	err := binary.Read(reader, binary.LittleEndian, &(b.val))
	if err != nil {
	    fmt.Println("binary.Read failed:", err)
	}
	return &b
}

func (b BooleanValue) getType() FleetDBType{
	return Boolean
}