package main

import (
	// "fmt"
	"flag"
	"fmt"

	"github.com/luc/tdfs"
	// "runtime"
	// "sync"
)

const DN_DIR_PREFIX string = "TinyDFS/DataNode"

func main() {
	id := flag.String("id", "1", "directory of chunks")
	capacity := flag.Int("capacity", tdfs.CHUNK_DN, "directory of chunks")
	local := flag.Bool("local", false, "is datanode run on host")

	flag.Parse()

	var dn tdfs.DataNode
	dn.DATANODE_DIR = DN_DIR_PREFIX + *id

	var DN_ADDR string
	if *local {
		DN_ADDR = "http://172.17.0.1:1109" + *id
	} else {
		DN_ADDR = fmt.Sprintf("http://datanode%s:1109%s", *id, *id)
	}

	dn.Reset()
	dn.SetConfig(DN_ADDR, *capacity)

	dn.Run()
}
