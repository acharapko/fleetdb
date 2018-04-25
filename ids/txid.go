package ids

import "strconv"

// ID represents a generic identifier in format of Zone.Node
type TXID uint64


func NewTXID(zone, node, txNum int) TXID {
	idz := uint64(zone)
	idz = idz << 48 //16 bit for txid.zone

	idn := uint64(node)
	idn = idn << 32 //16bit for txid.node

	id := idz
	id = id ^ idn;
	id = id ^ uint64(txNum)
	return TXID(id)
}

// Zone returns Zond ID component
func (i TXID) Zone() int {
	return int(i >> 48)
}

// Node returns Node ID component
func (i TXID) Node() int {
	idn := i & 0x0000FFFF00000000
	idn = idn >> 32

	return int(idn)
}

func (i TXID) ID() ID  {
	return ID(strconv.Itoa(i.Zone()) + "." + strconv.Itoa(i.Node()))
}

// TxNum returns TxNum ID component
func (i TXID) TxNum() int {
	idtxnum := i & 0x00000000FFFFFFFF
	return int(idtxnum)
}

func (i TXID) String() string {
	return strconv.Itoa(i.Zone()) + "." + strconv.Itoa(i.Node()) + "." + strconv.Itoa(i.TxNum())
}
