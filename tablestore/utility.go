package tablestore

import (
	"fmt"
	"io"
	"bytes"
	//"encoding/binary"
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
    //Handle Nil values
    
    kvItems := []KVItem{}
    var prefix []byte
    //var key []byte
    prefix = pKey
        
    if cKeyCount > 0{
	    prefix = append(prefix,cKey...)
    }
    if len(columnSpecs) == (pKeyCount + cKeyCount){
    	key := prefix
    	var val []byte
    	kvItems[0] = KVItem{key, val}
    }else{
    	//index := 0
	    for i, colSpec := range columnSpecs {
	    	if !colSpec.isPartition && !colSpec.isClustering{
	    		//fmt.Println(index)
	    		key := []byte{}
	    		key = append(key,prefix...)
	    		key = append(key,ColumnType)
	    		key = append(key, Text.getVal())
	    		key = append(key,[]byte(colSpec.colname + nullchar)...)
	    		val := values[i].Serialize()
	    		kvItems = append(kvItems, KVItem{key, val})
	    	}
		}
    }
	return kvItems;
}

//Incomplete Implementation
func parseKey(data []KVItem){
	for _, item := range data{
		r := bytes.NewReader(item.Key)
		p := make([]byte, 1)
		//var i uint8
		//fmt.Println(string(item.Key))
		_ , err := r.Read(p)
		for err != io.EOF{
			r.Read(p)
			//r.Read(p)
			switch p[0] {
				case 1:
					fmt.Printf("Integer")
				case 2:
					fmt.Printf("BigInt")
				case 5:
					//fmt.Printf("Text \n")
					text := NewTextValueFromNullTerminatedStream(r)
					fmt.Println(text.val)
			}
			_ , err = r.Read(p)
		}
	}
}

