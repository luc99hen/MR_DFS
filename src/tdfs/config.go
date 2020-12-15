package tdfs

import "sync"

/** Configurations for Pseudo Distributed Mode **/

/** Configurations for ALL Mode **/
const CHUNK_SIZE int = 1024 // 1kb per chunk
const REDUNDANCE int = 3
const CHUNK_DN int = 1000

// Chunk 一律表示逻辑概念，表示文件块
// Replica 表示文件块副本，是实际存储的
/** Data Structure **/
type ChunkUnit []byte // SPLIT_UNIT

// declared for return value type for namenode /getReplicaLocations
type ResFile struct {
	File  File `json:"File"`
	Exist bool `json:"exist"`
}

////// File to Chunk
type NameSpaceStruct struct {
	NameSpaceMap map[string]File
	mu           sync.Mutex
}
type File struct {
	Info             string      `json:"Info"`
	Size             int         `json:"Size"`
	Chunks           []FileChunk `json:"Chunks"`
	Offset_LastChunk int         `json:"Offset_LastChunk"`
}
type FileChunk [REDUNDANCE]ReplicaLocation
type ReplicaLocation struct {
	ServerLocation string `json:"ServerLocation"`
	ReplicaNum     int    `json:"ReplicaNum"`
}

type Client struct {
	NameNodeAddr string
	Mode         int
}

type NameNode struct {
	NameSpace    *NameSpaceStruct
	Location     string
	Port         int
	DNNumber     int
	DNLocations  []string
	DataNodes    []DataNode
	NAMENODE_DIR string
	REDUNDANCE   int
}
type DataNode struct {
	Location     string `json:"Location"` // http://IP:Port/
	Port         int    `json:"Port"`
	StorageTotal int    `json:"StorageTotal"` // a chunk as a unit
	StorageAvail int    `json:"StorageAvail"`
	ChunkAvail   []int  `json:"ChunkAvail"` //空闲块表
	LastEdit     int64  `json:"LastEdit"`
	DATANODE_DIR string `json:"DATANODE_DIR"`
}
type DNMeta struct {
	StorageTotal int `json:"StorageTotal"`
	StorageAvail int
	ChunkAvail   []int
	LastEdit     int64
}
