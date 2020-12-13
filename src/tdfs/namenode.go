package tdfs

import (
	// "time"
	"fmt"
	// "strconv"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// curl -X POST http://127.0.0.1:11090/putfile -F "putfile=@/Users/treasersmac/Programming/MilkPrairie/Gou/TinyDFS/SmallFile.txt"
// -H "Content-Type: multipart/form-data"
func (namenode *NameNode) Run() {
	router := gin.Default()

	router.POST("/getReplicaLocations", func(c *gin.Context) {

		// parse post parameter
		fileName := c.PostFormArray("fileName")[0]
		fileSize := c.PostFormArray("fileSize")[0]
		offsetLast := c.PostFormArray("offsetLast")[0]
		mode := c.PostFormArray("mode")[0]

		// check if fileName in namespace
		file, ok := namenode.NameSpace[fileName]
		defer func() {
			if r := recover(); r != nil {
				c.JSON(http.StatusNotAcceptable, ResFile{file, ok})
			}
		}()

		if ok {
			// if this file exist in namespace (request from client.getFile)
			fmt.Println("## file exist in namespace: ", fileName)
			if mode == "append" {
				var appendFile File

				appendFile.Info = "{name:" + fileName + "}"
				appendFile.Size, _ = strconv.Atoi(fileSize)
				appendFile.Offset_LastChunk, _ = strconv.Atoi(offsetLast)

				// allocate new chunks for appendFile
				for i := 0; i < getChunkLength(appendFile.Size); i++ {
					replicaLocationList := namenode.AllocateChunk()
					file.Chunks = append(file.Chunks, replicaLocationList)
					appendFile.Chunks = append(appendFile.Chunks, replicaLocationList)
				}

				// update target file metadata
				file.Size = appendFile.Size + file.Size
				file.Offset_LastChunk = appendFile.Offset_LastChunk
				namenode.NameSpace[fileName] = file

				fmt.Println("## append file allocated successfully ")

				c.JSON(http.StatusOK, ResFile{appendFile, ok})
			} else {
				c.JSON(http.StatusOK, ResFile{file, ok})
			}

		} else { // if file not exist
			fmt.Println("## file not exist in namespace: ", fileName)
			// if mode != put, return not found
			if mode != "put" {
				c.JSON(http.StatusNotFound, ResFile{file, ok})
				return
			}

			// if mode == put, create new file
			file.Info = "{name:" + fileName + "}"
			file.Size, _ = strconv.Atoi(fileSize)
			file.Offset_LastChunk, _ = strconv.Atoi(offsetLast)

			for i := 0; i < getChunkLength(file.Size); i++ {
				replicaLocationList := namenode.AllocateChunk()
				file.Chunks = append(file.Chunks, replicaLocationList)
			}

			namenode.NameSpace[fileName] = file
			fmt.Println("## new file allocated successfully", fileName)
			c.JSON(http.StatusOK, ResFile{file, ok})
		}
	})

	// router.DELETE GET
	router.DELETE("/delfile/:fileName", func(c *gin.Context) {
		fileName := c.Param("fileName")
		file := namenode.NameSpace[fileName]
		for i := 0; i < getChunkLength(file.Size); i++ {
			namenode.DelChunk(file, fileName, i)
		}
		c.String(http.StatusOK, "DelFile:"+fileName+" SUCCESS\n")
	})

	router.GET("/test", func(c *gin.Context) {
		namenode.GetDNMeta() //namenode.ShowInfo()
	})

	router.Run(":" + strconv.Itoa(namenode.Port))
}

func (namenode *NameNode) DelChunk(file File, fileName string, num int) { //ChunkUnit chunkbytes []byte
	fmt.Println("** deleting chunk-", num, "of file:", fileName)
	for i := 0; i < REDUNDANCE; i++ {
		chunklocation := file.Chunks[num][i].ServerLocation
		chunknum := file.Chunks[num][i].ReplicaNum
		url := chunklocation + "/delchunk/" + strconv.Itoa(chunknum)

		// response, err := http.Get(url)
		c := &http.Client{}
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			fmt.Println("XXX NameNode error at Del chunk of ", file.Info, ": ", err.Error())
			TDFSLogger.Panic("XXX NameNode error: ", err)
		}

		response, err := c.Do(req)
		if err != nil {
			fmt.Println("XXX NameNode error at Del chunk(Do):", err.Error())
			TDFSLogger.Panic("XXX NameNode error at Del chunk(Do):", err)
		}
		defer response.Body.Close()

		/** Read response **/
		delRes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response", err.Error())
			TDFSLogger.Panic("XXX NameNode error: ", err)
		}
		fmt.Println("*** DataNode Response of Delete chunk-", num, "replica-", i, ": ", string(delRes))
		// return chunkbytes
	}
}

func (namenode *NameNode) AllocateChunk() (rlList [REDUNDANCE]ReplicaLocation) {
	var max int
	for i := 0; i < namenode.REDUNDANCE; i++ {
		max = 0
		for j := 0; j < namenode.DNNumber; j++ {
			if namenode.DataNodes[j].StorageAvail > namenode.DataNodes[max].StorageAvail {
				max = j
			}
		}

		if len(namenode.DataNodes[max].ChunkAvail) == 0 {
			fmt.Println("XXX NameNode error: no available datanode")
			TDFSLogger.Panic("XXX NameNode error: no available datanode")
		}

		// choose the datanode which has max available capacity
		rlList[i].ServerLocation = namenode.DataNodes[max].Location
		rlList[i].ReplicaNum = namenode.DataNodes[max].ChunkAvail[0]
		n := namenode.DataNodes[max].StorageAvail

		// update datanode metadata
		namenode.DataNodes[max].ChunkAvail[0] = namenode.DataNodes[max].ChunkAvail[n-1]
		namenode.DataNodes[max].ChunkAvail = namenode.DataNodes[max].ChunkAvail[0 : n-1]
		namenode.DataNodes[max].StorageAvail--
	}

	return rlList
}

func (namenode *NameNode) Reset() {

	// CleanFile("TinyDFS/DataNode1/chunk-"+strconv.Itoa(i))
	fmt.Println("# Reset...")

	err := os.RemoveAll(namenode.NAMENODE_DIR + "/")
	if err != nil {
		fmt.Println("XXX NameNode error at RemoveAll dir", err.Error())
		TDFSLogger.Panic("XXX NameNode error: ", err)
	}

	err = os.MkdirAll(namenode.NAMENODE_DIR, 0777)
	if err != nil {
		fmt.Println("XXX NameNode error at MkdirAll", err.Error())
		TDFSLogger.Panic("XXX NameNode error: ", err)
	}

}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string) {
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println("XXX NameNode error at Atoi parse Port", err.Error())
		TDFSLogger.Panic("XXX NameNode error: ", err)
	}

	ns := NameSpaceStruct{}
	namenode.NameSpace = ns
	namenode.Port = res
	namenode.Location = location
	namenode.DNNumber = dnnumber
	namenode.DNLocations = dnlocations
	namenode.REDUNDANCE = redundance
	fmt.Println("************************************************************")
	fmt.Println("************************************************************")
	fmt.Printf("*** Successfully Set Config data for the namenode\n")
	namenode.ShowInfo()
	fmt.Println("************************************************************")
	fmt.Println("************************************************************")
}

func (namenode *NameNode) ShowInfo() {
	fmt.Println("************************************************************")
	fmt.Println("****************** showinf for NameNode ********************")
	fmt.Printf("Location: %s\n", namenode.Location)
	fmt.Printf("DATANODE_DIR: %s\n", namenode.NAMENODE_DIR)
	fmt.Printf("Port: %d\n", namenode.Port)
	fmt.Printf("DNNumber: %d\n", namenode.DNNumber)
	fmt.Printf("REDUNDANCE: %d\n", namenode.REDUNDANCE)
	fmt.Printf("DNLocations: %s\n", namenode.DNLocations)
	fmt.Printf("DataNodes: ")
	fmt.Println(namenode.DataNodes)
	fmt.Println("******************** end of showinfo ***********************")
	fmt.Println("************************************************************")
}

func (namenode *NameNode) GetDNMeta() { // UpdateMeta
	for i := 0; i < len(namenode.DNLocations); i++ {
		response, err := http.Get(namenode.DNLocations[i] + "/getmeta")
		if err != nil {
			fmt.Println("XXX NameNode error at Get meta of ", namenode.DNLocations[i], ": ", err.Error())
			TDFSLogger.Panic("XXX NameNode error: ", err)
		}
		defer response.Body.Close()
		// bytes, err := ioutil.ReadAll(response.Body)
		// if err != nil {fmt.Println("NameNode error at read responsed meta of ", namenode.DNLocations[i],": ", err.Error())}
		// fmt.Print("datanode", i, " metadata: ")
		// fmt.Printf("%s\n", bytes)

		var dn DataNode
		err = json.NewDecoder(response.Body).Decode(&dn)
		if err != nil {
			fmt.Println("XXX NameNode error at decode response to json.", err.Error())
			TDFSLogger.Panic("XXX NameNode error: ", err)
		}
		// fmt.Println(dn)
		// err = json.Unmarshal([]byte(str), &dn)
		namenode.DataNodes = append(namenode.DataNodes, dn)
	}
	namenode.ShowInfo()
}
