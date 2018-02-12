package fleetdb

import (
	"fmt"
	"github.com/acharapko/fleetdb/utils/hlc"
)



type MsgHeader struct {
	HLCTime		hlc.Timestamp
}

func (hdr MsgHeader) String() string {
	return fmt.Sprintf("MSG HEADER {hlcint=%d}", hdr.HLCTime)
}

func (hdr* MsgHeader) ToBytes() []byte {
	return hdr.HLCTime.ToBytes()
}

func NewMsgHeaderFromBytes(bts []byte) *MsgHeader {
	ts := hlc.NewTimestampBytes(bts)
	hdr := MsgHeader{HLCTime:*ts}
	return &hdr;
}