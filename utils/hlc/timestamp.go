package hlc

import (
	"math"
	"time"
	"encoding/binary"
)

// Timestamp constant values.
var (
	// MaxTimestamp is the max value allowed for Timestamp.
	MaxTimestamp = Timestamp{physicalTime: math.MaxInt64, logicalTime: math.MaxInt16}
	// MinTimestamp is the min value allowed for Timestamp.
	MinTimestamp = Timestamp{physicalTime: 0, logicalTime: 0}
)

type Timestamp struct {
	physicalTime 	int64
	logicalTime		int16
}

func (t Timestamp) ToInt64() int64 {
	i64hlc := t.physicalTime
	i64hlc = i64hlc << 16 //get some room for lc
	i64hlc = i64hlc | int64(t.logicalTime) //slap lc to last 16 bits which we cleared
	return i64hlc
}

func (t Timestamp) ToBytes() []byte {
	tint := t.ToInt64()

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(tint))

	return b
}

func NewTimestampI64(hlc int64) *Timestamp {
	wallTime := hlc >> 16;
	logical := int16(hlc & 0x00000000000000FFFF);
	t := Timestamp{physicalTime:wallTime, logicalTime:logical}
	return &t
}

func NewTimestampBytes(ts []byte) *Timestamp {
	tint := int64(binary.LittleEndian.Uint64(ts))
	return NewTimestampI64(tint)
}

func NewTimestampPt(pt int64) *Timestamp {
	t := Timestamp{physicalTime:pt, logicalTime:0}
	return &t
}

func NewTimestamp(pt int64, lc int16) *Timestamp {
	t := Timestamp{physicalTime:pt, logicalTime:lc}
	return &t
}

func (t Timestamp) GetPhysicalTime() int64 {
	return t.physicalTime
}

func (t Timestamp) GetLogicalTime() int16 {
	return t.logicalTime
}

func (t *Timestamp) IncrementLogical() {
	t.logicalTime++
}

func (t *Timestamp) ResetLogical() {
	t.logicalTime = 0
}

func (t *Timestamp) SetPhysicalTime(pt int64) {
	t.physicalTime = pt
}

func (t *Timestamp) SetLogicalTime(lc int16) {
	t.logicalTime = lc;
}

func (t *Timestamp) Compare(t2 *Timestamp) int {
	if t.physicalTime > t2.physicalTime {
		return 1
	}
	if t.physicalTime < t2.physicalTime {
		return -1
	}
	if t.logicalTime > t2.logicalTime {
		return 1
	}
	if t.logicalTime < t2.logicalTime {
		return -1
	}

	return 0
}

// GoTime converts the timestamp to a time.Time.
func (t Timestamp) GoTime() time.Time {
	return time.Unix(0, t.physicalTime)
}