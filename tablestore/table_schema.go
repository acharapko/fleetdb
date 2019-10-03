package tablestore

import (

)

type FleetDbColumnSpec struct {
    colname         string
    coltype         FleetDBType
    isPartition     bool
    isClustering    bool
}
