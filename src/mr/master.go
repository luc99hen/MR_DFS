package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/luc/tdfs"
)

func (master *Master) Run() {
	router := gin.Default()
	router.MaxMultipartMemory = 1024 << 20

	router.POST("/startup", func(c *gin.Context) {

		fileName := master.recvFile(c)

		// assign map tasks to workers based on chunk locations
		chunks := dfsClient.PutFile(MR_DIR + "master/" + fileName)
		for _, chunk := range chunks {
			//by default choose first replica of all redundances for mapper
			master.addChunkToMapper(chunk[0])
		}

		// start MAP phase
		signals := make(chan Worker)
		for _, mapper := range master.Mappers {
			// async call for each mapper
			go StartMapper(mapper, signals)
		}

		// Wait for all mapper finish, and check their state
		for i := 0; i < len(master.Mappers); i++ {
			mapper := <-signals
			fmt.Println("Mapper State: ", mapper)
			if !mapper.State {
				MyPanic("XX Mapper status fail: ", mapper)
			}
		}

		// start REDUCE phase
		for id, reducer := range master.Reducers {
			go StartReducer(reducer, id, signals)
		}

		// Wait for all reducer finish, and check their state
		for i := 0; i < len(master.Reducers); i++ {
			reducer := <-signals
			fmt.Println("Reducer State: ", reducer)
			if !reducer.State {
				MyPanic("XX Reducer status fail: ", reducer)
			}
		}

		// read output file at mrout
		dfsClient.GetFile("mrout")

	})

	router.Run(":" + master.Addr)

}

// recFile receive file from post request and store it locally
func (master *Master) recvFile(c *gin.Context) string {
	file, header, err := c.Request.FormFile("putfile") // no multipart boundary param in Content-Type
	if err != nil {
		c.String(http.StatusBadRequest, "Bad request")
		MyPanic("XXX Master error at Request FormFile ", err.Error())
		return "null"
	}

	filename := tdfs.Path2Name(header.Filename)
	fmt.Println(file, err, filename)

	path := MR_DIR + "master/"
	tdfs.CheckPath(path)
	out, err := os.Create(path + filename)
	if err != nil {
		c.String(http.StatusBadRequest, "Master error at Create file")
		MyPanic("XXX Master error: at Create ", err)
		return "null"
	}
	defer out.Close()

	_, err = io.Copy(out, file) //transfer file from networkr to local
	if err != nil {
		c.String(http.StatusBadRequest, "Master error at Copy file")
		MyPanic("XXX Master error at Copy", err.Error())
		return "null"
	}

	c.String(http.StatusOK, "StartUp SUCCESS\n")
	return filename

}

func (master *Master) SetConfig(masterAddr string) {
	for i := 0; i < REDUCER_NUM; i++ {
		// master.Reducers[i] = &Reducer{"http://172.17.0.1:11101", false}
		master.Reducers[i] = &Reducer{fmt.Sprintf("http://datanode%d:%d", i+1, WORKER_PORT), false}
	}
	master.Addr = masterAddr
}

func StartMapper(mapper *Mapper, signal chan Worker) {
	if mapper.Worker.State {
		// something wrong here, try to start a mapper already started
		MyPanic("XX try to start a mapper already started")
	}
	// send mapper.Chunks to mapper.WorkerAddr
	parts := strings.Split(mapper.Worker.WorkerAddr, ":")
	url := parts[0] + ":" + parts[1] + ":" + strconv.Itoa(WORKER_PORT)
	chunks, err := json.Marshal(mapper.Chunks)
	if err != nil {
		MyPanic("XXX Master error at JSON encode chunk array ", err.Error())
	}

	response, err := http.Post(url+"/doMap", "application/json", bytes.NewBuffer(chunks))
	if err != nil || response.StatusCode != http.StatusOK {
		MyPanic("XXX Master error at send request /doMap ", err)
	}
	defer response.Body.Close()

	mapper.Worker.State = true
	signal <- mapper.Worker
}

func StartReducer(reducer *Reducer, id int, signal chan Worker) {
	if reducer.State {
		// something wrong here, try to start a mapper already started
		MyPanic("XX try to start a mapper already started")
	}

	host := reducer.WorkerAddr
	data := url.Values{
		"id": {strconv.Itoa(id)},
	}
	response, err := http.PostForm(host+"/doReduce", data)
	if err != nil || response.StatusCode != http.StatusOK {
		MyPanic("XXX Master error at send request /doReduce ", err)
	}
	defer response.Body.Close()

	reducer.State = true
	signal <- Worker(*reducer)
}

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
		master.Mappers = append(master.Mappers, &mapper)
	}
}
