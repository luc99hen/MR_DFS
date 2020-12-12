package tdfs

import (
	// "time"
	"fmt"
	// "strconv"
	"bytes"
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

		fileName := c.PostFormArray("fileName")[0]
		fileSize := c.PostFormArray("fileSize")[0]
		offsetLast := c.PostFormArray("offsetLast")[0]

		var file File
		file.Info = "{name:" + fileName + "}"
		file.Size, _ = strconv.Atoi(fileSize)
		file.Offset_LastChunk, _ = strconv.Atoi(offsetLast)

		for i := 0; i < getChunkLength(file.Size); i++ {
			replicaLocationList := namenode.AllocateChunk()
			file.Chunks = append(file.Chunks, replicaLocationList)
		}

		namenode.NameSpace[fileName] = file
		fmt.Println("## replicaLocation allocated successfully", file)
		c.JSON(http.StatusOK, file)

	})

	router.GET("/getfile/:fileName", func(c *gin.Context) {
		fileName := c.Param("fileName")
		fmt.Println("$ getfile ...", fileName)
		file := namenode.NameSpace[fileName]
		// fmt.Println(file)
		/* 从DN中读取file所在的块数据，将块数据暂存在一个文件夹下 */
		for i := 0; i < getChunkLength(file.Size); i++ {
			namenode.GetChunk(file, fileName, i)
		}
		/* 将文件夹下的块数据整合成一个文件 */
		// fdata := bytes.Join(filedata, nil)
		fdata := namenode.AssembleFile(file, fileName)
		/* 将该文件发送给客户端 */
		c.String(http.StatusOK, string(fdata))
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

func (namenode *NameNode) AssembleFile(file File, fileName string) []byte {
	fmt.Println("@ AssembleFile of ", fileName)
	chunkLength := getChunkLength(file.Size)
	filedata := make([][]byte, chunkLength)
	for i := 0; i < chunkLength; i++ {
		// fmt.Println("& Assmble chunk-",i)
		b := readFileByBytes(namenode.NAMENODE_DIR + "/" + getHash([]byte(fileName)) + "/chunk-" + strconv.Itoa(i))
		// fmt.Println("& Assmble chunk b=", b)
		filedata[i] = make([]byte, SPLIT_UNIT)
		filedata[i] = b
		// fmt.Println("& Assmble chunk filedata[i]=", filedata[i])
		// fmt.Println("& Assmble chunk-",i)
	}
	fdata := bytes.Join(filedata, nil)
	FastWrite(namenode.NAMENODE_DIR+"/nn-"+fileName, fdata)
	return fdata
}

func (namenode *NameNode) GetChunk(file File, fileName string, num int) { //ChunkUnit chunkbytes []byte

	fmt.Println("* getting chunk-", num, "of file:", fileName)

	for i := 0; i < REDUNDANCE; i++ {
		replicalocation := file.Chunks[num][i].ServerLocation
		repilcanum := file.Chunks[num][i].ReplicaNum
		/* send Get chunkdata request */
		url := replicalocation + "/getchunk/" + strconv.Itoa(repilcanum)
		dataRes, err := http.Get(url)
		if err != nil {
			fmt.Println("XXX NameNode error at Get chunk of ", file.Info, ": ", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer dataRes.Body.Close()
		/* deal response of Get */
		chunkbytes, err := ioutil.ReadAll(dataRes.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at ReadAll response of chunk", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		fmt.Println("** DataNode Response of Get chunk-", num, ": ", string(chunkbytes))
		/* store chunkdata at nn local */
		FastWrite(namenode.NAMENODE_DIR+"/"+getHash([]byte(fileName))+"/chunk-"+strconv.Itoa(num), chunkbytes)

		/* send Get chunkhash request */
		hashRes, err := http.Get(replicalocation + "/getchunkhash/" + strconv.Itoa(repilcanum))
		if err != nil {
			fmt.Println("XXX NameNode error at Get chunkhash", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer hashRes.Body.Close()
		/* deal Get chunkhash request */
		chunkhash, err := ioutil.ReadAll(hashRes.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response of chunkhash", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		/* check hash */
		hashStr := getHash(chunkbytes)
		fmt.Println("*** chunk hash calculated: ", hashStr)
		fmt.Println("*** chunk hash get: ", string(chunkhash))

		if hashStr == string(chunkhash) {
			break
		} else {
			fmt.Println("X=X the first replica of chunk-", num, "'s hash(checksum) is WRONG, continue to request anothor replica...")
			continue
		}
	}
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
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		response, err := c.Do(req)
		if err != nil {
			fmt.Println("XXX NameNode error at Del chunk(Do):", err.Error())
			TDFSLogger.Fatal("XXX NameNode error at Del chunk(Do):", err)
		}
		defer response.Body.Close()

		/** Read response **/
		delRes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
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
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

	err = os.MkdirAll(namenode.NAMENODE_DIR, 0777)
	if err != nil {
		fmt.Println("XXX NameNode error at MkdirAll", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
	}

}

func (namenode *NameNode) SetConfig(location string, dnnumber int, redundance int, dnlocations []string) {
	temp := strings.Split(location, ":")
	res, err := strconv.Atoi(temp[2])
	if err != nil {
		fmt.Println("XXX NameNode error at Atoi parse Port", err.Error())
		TDFSLogger.Fatal("XXX NameNode error: ", err)
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
			TDFSLogger.Fatal("XXX NameNode error: ", err)
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
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		// fmt.Println(dn)
		// err = json.Unmarshal([]byte(str), &dn)
		namenode.DataNodes = append(namenode.DataNodes, dn)
	}
	namenode.ShowInfo()
}

func (namenode *NameNode) PutDNMeta() {
	// 把namenode.DataNodes传给各个DataNodes
	// 没必要了，因为给DN发送存储位置信息时现在DN会进行更新了。
}
