package mr

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/luc/tdfs"
)

func (master *Master) Run() {
	router := gin.Default()
	router.MaxMultipartMemory = 1024 << 20

	router.POST("/startup", func(c *gin.Context) {

		var client tdfs.Client
		client.SetConfig("http://namenode:11090")

		// assign map tasks to workers based on chunk locations
		chunks := client.PutFile("uploadFilePath")
		for _, chunk := range chunks {
			//by default choose first replica of all redundances for mapper
			master.addChunkToMapper(chunk[0])
		}

		// start MAP phase
		signals := make(chan Worker)
		for _, mapper := range master.Mappers {
			// async call for each mapper
			go StartMapper(&mapper, signals)
		}

		// Wait for all mapper finish, and show their state
		for i := 0; i < len(master.Mappers); i++ {
			mapper := <-signals
			fmt.Println("Mapper State: ", mapper)
			if !mapper.State {
				// error
				MyFatal("XX Mapper status fail: ", mapper)
			}
		}

		// start REDUCE phase
		for _, reducer := range master.Reducers {
			go StartReducer(&reducer)
		}

		// read output file at mrout
		client.GetFile("mrout")

	})

	router.Run(":" + strings.Split(master.Addr, ":")[0])

}

func (master *Master) SetConfig(reducerNum int, masterAddr string) {
	for i := 0; i < reducerNum; i++ {
		master.Reducers[i] = Reducer{fmt.Sprintf("http://datanode%d:1110%d", i, i), false}
	}
	master.Addr = masterAddr
}

func StartMapper(mapper *Mapper, signal chan Worker) {
	if mapper.Worker.State {
		// something wrong here, try to start a mapper already started
		signal <- mapper.Worker
		MyFatal("XX try to start a mapper already started")
	}
	// send mapper.Chunks to mapper.WorkerAddr
	mapper.Worker.State = true
	signal <- mapper.Worker
}

func StartReducer(reducer *Reducer) {}

func (master *Master) addChunkToMapper(chunk tdfs.ReplicaLocation) {
	found := false
	for _, mapper := range master.Mappers {
		if mapper.Worker.WorkerAddr == chunk.ServerLocation {
			mapper.Chunks = append(mapper.Chunks, chunk.ReplicaNum)
			found = true
			break
		}
	}
	if !found {
		mapper := Mapper{Worker{chunk.ServerLocation, false}, []int{chunk.ReplicaNum}}
		master.Mappers = append(master.Mappers, mapper)
	}
}
