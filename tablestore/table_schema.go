package tablestore

import (

)

type FleetDbColumnSpec struct {
    colname         string
    coltype         FleetDBValue
    isPartition     bool
    isClustering    bool
    seqNo			uint8
}
