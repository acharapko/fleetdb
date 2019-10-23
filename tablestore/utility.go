package tablestore

import (
)

const(
	PartitionType = iota + 1;
	ClusteringType
	ColumnType
)

var nullchar string = "\000"

func TranslateToKV(columnSpecs []FleetDbColumnSpec, values []FleetDBValue) []KVItem{
    var pKey []byte
    var cKey []byte
    pKeyCount := 0
    cKeyCount := 0
    for i, colSpec := range columnSpecs {
    	if colSpec.isPartition{
    		pKey = append(pKey,PartitionType)
    		coltype := colSpec.coltype.getType()
    		pKey = append(pKey, coltype.getVal())
    		if coltype == Text{
    			pKey = append(pKey,[]byte(values[i].String()+ nullchar)...)
    		}else{
    			pKey = append(pKey,values[i].Serialize()...)
    		}
    		pKeyCount = pKeyCount + 1
    	}
    	
    	if colSpec.isClustering{
    		cKey = append(cKey,ClusteringType)
    		coltype := colSpec.coltype.getType()
    		cKey = append(cKey, coltype.getVal())
    		if coltype == Text{
    			cKey = append(cKey,[]byte(values[i].String() + nullchar)...)
    		}else{
    			cKey = append(cKey,values[i].Serialize()...)
    		}
    		cKeyCount = cKeyCount + 1
    	}
   }
    kvItems := []KVItem{}
    var prefix []byte
    prefix = pKey
        
    if cKeyCount > 0{
	    prefix = append(prefix,cKey...)
    }
    if len(columnSpecs) == (pKeyCount + cKeyCount){
    	key := prefix
    	var val []byte
    	kvItems = append(kvItems, KVItem{key, val})
    }else{
	    for i, colSpec := range columnSpecs {
	    	if !colSpec.isPartition && !colSpec.isClustering{
	    		//fmt.Println(index)
	    		key := []byte{}
	    		key = append(key,prefix...)
	    		key = append(key,ColumnType)
	    		key = append(key, Text.getVal())
	    		key = append(key,[]byte(colSpec.colname + nullchar)...)
	    		var val []byte
	    		if(values[i] != nil){
		    		val = values[i].Serialize()
	    		}else {
	    			val = nil
	    		}
	    		kvItems = append(kvItems, KVItem{key, val})
	    	}
		}
    }
	return kvItems;
}
