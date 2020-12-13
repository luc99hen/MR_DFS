package main

import (
	// "fmt"
	"github.com/luc/tdfs"
	// "runtime"
	// "sync"
)

const NN_DIR string = "TinyDFS/NameNode"
const NN_LOCATION string = "http://namenode:11090"

func main() {
	var nn tdfs.NameNode
	nn.NAMENODE_DIR = NN_DIR
	dnlocations := []string{"http://datanode1:11091", "http://datanode2:11092", "http://datanode3:11093", "http://datanode4:11094", "http://datanode5:11095"}

	nn.Reset()
	nn.SetConfig(NN_LOCATION, len(dnlocations), dnlocations)
	nn.GetDNMeta() // UpdateMeta

	nn.Run()
}
