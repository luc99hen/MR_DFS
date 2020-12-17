package mr

import "github.com/luc/tdfs"

// Mapper is assigned based on DFS chunk locations, MAPPER_NUM cann't be specified here
// const MAPPER_NUM = 3
const REDUCER_NUM = 5
const WORKER_PORT = 11101

type Worker struct {
	WorkerAddr string
	State      bool
}

type Mapper struct {
	Worker Worker
	Chunks []int
}

type Reducer Worker

type Master struct {
	Mappers  []*Mapper
	Reducers [REDUCER_NUM]*Reducer
	Addr     string
}

const MR_DIR = "MR/"

var dfsClient tdfs.Client = tdfs.Client{
	NameNodeAddr: "http://namenode:11090",
	Mode:         0,
}

type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
