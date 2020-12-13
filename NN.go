package main

import (
	// "fmt"
	"github.com/luc/tdfs"
	// "runtime"
	// "sync"
)

const NN_DIR string = "TinyDFS/NameNode"
const NN_LOCATION string = "http://localhost:11090"
const rEDUNDANCE int = 2

func main() {
	var nn tdfs.NameNode
	nn.NAMENODE_DIR = NN_DIR
	dnlocations := []string{"http://datanode1:11091", "http://datanode2:11092", "http://172.17.0.1:11093"}

	nn.Reset()
	nn.SetConfig(NN_LOCATION, len(dnlocations), rEDUNDANCE, dnlocations)
	nn.GetDNMeta() // UpdateMeta

	nn.Run()
}
