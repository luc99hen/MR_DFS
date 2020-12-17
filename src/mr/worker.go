package mr

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/luc/tdfs"
)

func (worker *Worker) Run() {
	router := gin.Default()
	router.MaxMultipartMemory = 1024 << 20

	router.POST("/doMap", func(c *gin.Context) {
		// get chunk id from paras
		var chunks []int
		err := c.BindJSON(&chunks)
		if err != nil {
			MyPanic("XX Worker fail at doMap when parsing parameters ", err)
		}

		// call user-defined mapper function, write out	ut to mout-i
		const inputLocation = "TinyDFS/DataNode1/chunk-"
		for _, id := range chunks {
			fileName := inputLocation + strconv.Itoa(id)
			contents := tdfs.ReadFileByBytes(fileName)
			intermediate := Map(fileName, string(contents)) //Map is defined by Users
			// split intermediate by partitioner,
			var outputLines [REDUCER_NUM][]string
			for _, pair := range intermediate {
				partition := Partition(pair.Key, REDUCER_NUM) //Partition Strategy can be changed
				outputLines[partition] = append(outputLines[partition], fmt.Sprintf("%s %s", pair.Key, pair.Value))
			}
			// and append to file mout-i
			for i := range outputLines {
				err := tdfs.WriteFile(fmt.Sprintf("MR/mout-%d", i), outputLines[i])
				if err != nil {
					MyPanic("XX Mapper fail at write kvs to local file", err)
				}
			}
		}

		// append mout-i to rin-i in dfs
		var wg sync.WaitGroup
		for i := 0; i < REDUCER_NUM; i++ {
			wg.Add(1)
			go func(id int) {
				dfsClient.AppendFile(fmt.Sprintf("MR/mout-%d", id), fmt.Sprintf("rin-%d", id))
				wg.Done()
			}(i)
		}
		wg.Wait()

		c.String(200, "Map Success")
	})

	router.POST("/doReduce", func(c *gin.Context) {
		// read rin-i from dfs
		id := c.PostFormArray("id")[0]
		contents, _ := dfsClient.GetFile("rin-" + id)
		byteReader := bytes.NewBuffer(contents)
		var intermediate []KeyValue
		for {
			line, err := byteReader.ReadString('\n')
			line = strings.TrimSpace(line)
			if err != nil {
				if err == io.EOF {
					break
				}
				c.String(500, "Reduce Fail")
				MyPanic("XX doReduce fail at read bytes ", err)
			}

			kv := strings.Split(line, " ")
			if len(kv) == 2 { // handle padding
				intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
			}
		}

		// sort by key, and combine write output to rout-i
		sort.Sort(ByKey(intermediate))
		outputFileName := "MR/rout-" + id
		ofile, _ := os.Create(outputFileName)
		defer ofile.Close()

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := Reduce(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		// append rout-i to mrout
		dfsClient.AppendFile(outputFileName, "mrout")

		c.String(200, "Reduce Success")
	})

	router.Run(":" + strings.Split(worker.WorkerAddr, ":")[2])
}
