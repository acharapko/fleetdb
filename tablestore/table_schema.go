package tablestore

import (

)
type FleetDBType uint8

const (
  INT FleetDBType = iota
  FLOAT
  TEXT
)

type FleetDbColumnSpec struct {
    colname         string
    coltype         FleetDBType
    isPartition     bool
    isClustering    bool
}
