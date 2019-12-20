package tablestore

import (
	"fmt"
)
type FleetDBTableSpec struct {
    columnSpecs map[string]*FleetDbColumnSpec 
}

func (s *FleetDBTableSpec) GetColumnSpec(column string) *FleetDbColumnSpec {
    if column == ""{
    	fmt.Println("Table name is Nil")
    	return nil
    }
    return s.columnSpecs[column]
}
