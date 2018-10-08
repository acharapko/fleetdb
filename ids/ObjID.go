package ids

import (
	"encoding/binary"
	"encoding/base64"
	"fmt"
)

type Key []byte

type ObjID struct {
	Key 	Key
	Table 	string

	IsRange bool
}

func NewObjID(k Key, table string, isRange bool) *ObjID {
	return &ObjID{Key:k, Table:table, IsRange:isRange}
}

func (objID *ObjID) Clone() *ObjID {
	return NewObjID(objID.Key, objID.Table, objID.IsRange)
}

/**
 These must translate key to byte array preserving the order
 */

func ObjIDFromInt64(k int64, table string, isRange bool) *ObjID {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(k))
	return &ObjID{Key:b, Table:table, IsRange:isRange}
}

func ObjIDFromFromInt(k int, table string, isRange bool) *ObjID {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(k))
	return &ObjID{Key:b, Table:table, IsRange:isRange}
}

func ObjIDFromString(k string, table string, isRange bool) *ObjID {
	return &ObjID{Key:[]byte(k), Table:table, IsRange:isRange}
}

/**
A Base64 encoding of the key
 */

func (objID *ObjID) B64() (string){
	return base64.StdEncoding.EncodeToString(objID.Key)
}

func ObjIDFromB64(k string, table string, isRange bool) *ObjID {
	kb, err := base64.StdEncoding.DecodeString(k)

	if err == nil {
		return &ObjID{Key:kb, Table:table, IsRange:isRange}
	}
	return nil
}

func (objID *ObjID) String() string {
	return fmt.Sprintf("ObjID{key=%v, TableName=%s, range=%t}", string(objID.Key), objID.Table, objID.IsRange)
}

/**
a table may have multiple buckets (levelDB instances)
TODO: deprecate this
*/

func (objID *ObjID) Bucket(numBuckets int) int {
	var tempByte byte;

	for _, b := range objID.Key {
		tempByte = tempByte ^ b
	}
	bucket := int(tempByte) % numBuckets
	return bucket
}
