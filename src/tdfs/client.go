package tdfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

func (client *Client) DelFile(fName string) {
	fmt.Println("****************************************")
	fmt.Printf("*** Deleting from TDFS [NameNode: %s] of /%s )\n", client.NameNodeAddr, fName)

	// Create client
	c := &http.Client{}
	// Create request
	req, err := http.NewRequest("DELETE", client.NameNodeAddr+"/delfile/"+fName, nil)
	if err != nil {
		fmt.Println("XXX Client error at del file(NewRequest):", err.Error())
		TDFSLogger.Fatal("XXX Client error at del file(NewRequest):", err)
	}
	// Fetch Request
	response, err := c.Do(req)
	if err != nil {
		fmt.Println("XXX Client error at del file(Do):", err.Error())
		TDFSLogger.Fatal("XXX Client error at del file(Do):", err)
	}
	defer response.Body.Close()
	// Read Response Body
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("XXX Client error at read response data", err.Error())
		TDFSLogger.Fatal("XXX Client error at read response data", err)
	}

	fmt.Print("*** NameNode Response: ", string(bytes))
	fmt.Println("****************************************")
}

func (client *Client) Test() {
	// _, err := http.Get(client.NameNodeAddr + "/test")
	// if err != nil {
	// 	fmt.Println("Client error at Get test", err.Error())
	// }
	client.PutFile("./data/BFile.txt")
	// client.test("BFile.txt")
}

// GetFile :download file from DFS
func (client *Client) GetFile(fileName string) {
	dummyParas := 0
	resFile := client.getReplicaLocations(fileName, dummyParas, dummyParas)

	if !resFile.Exist {
		fmt.Println("XXX Client error at GetFile, file not exist")
		TDFSLogger.Fatal("XXX Client error at GetFile, file not exist")
		return
	}

	GetChunks(&resFile.File, fileName)
	AssembleChunks(&resFile.File, fileName)

}

func AssembleChunks(file *File, fileName string) {
	fmt.Println("@ AssembleFile of ", fileName)
	chunkLength := getChunkLength(file.Size)
	filedata := make([][]byte, chunkLength)
	for i := 0; i < chunkLength; i++ {
		// fmt.Println("& Assmble chunk-",i)
		b := readFileByBytes("./" + getHash([]byte(fileName)) + "/chunk-" + strconv.Itoa(i))
		// fmt.Println("& Assmble chunk b=", b)
		filedata[i] = make([]byte, SPLIT_UNIT)
		filedata[i] = b
		// fmt.Println("& Assmble chunk filedata[i]=", filedata[i])
		// fmt.Println("& Assmble chunk-",i)
	}

	os.RemoveAll("./" + getHash([]byte(fileName)))

	fdata := bytes.Join(filedata, nil)
	FastWrite("./local-"+fileName, fdata)
	fmt.Print("*** complete file: ", string(fdata))
}

// GetChunks pulls all the file chunk from datanode and store it in a temporary directory in clients
func GetChunks(file *File, fileName string) { //ChunkUnit chunkbytes []byte

	for num := 0; num < getChunkLength(file.Size); num++ {
		fmt.Println("* getting chunk-", num, "of file:", fileName)
		for i := 0; i < REDUNDANCE; i++ {
			replicalocation := file.Chunks[num][i].ServerLocation
			repilcanum := file.Chunks[num][i].ReplicaNum
			/* send Get chunkdata request */
			url := replicalocation + "/getchunk/" + strconv.Itoa(repilcanum)
			dataRes, err := http.Get(url)
			if err != nil {
				fmt.Println("XXX Client error at Get chunk of ", file.Info, ": ", err.Error())
				TDFSLogger.Fatal("XXX Client error: ", err)
			}
			defer dataRes.Body.Close()
			/* deal response of Get */
			chunkbytes, err := ioutil.ReadAll(dataRes.Body)
			if err != nil {
				fmt.Println("XXX Client error at ReadAll response of chunk", err.Error())
				TDFSLogger.Fatal("XXX Client error: ", err)
			}
			fmt.Println("** DataNode Response of Get chunk-", num, ": ", string(chunkbytes))
			/* store chunkdata at nn local */

			tmpChunkPath := "./" + getHash([]byte(fileName))
			CheckPath(tmpChunkPath)
			FastWrite(tmpChunkPath+"/chunk-"+strconv.Itoa(num), chunkbytes)

			/* send Get chunkhash request */
			hashRes, err := http.Get(replicalocation + "/getchunkhash/" + strconv.Itoa(repilcanum))
			if err != nil {
				fmt.Println("XXX Client error at Get chunkhash", err.Error())
				TDFSLogger.Fatal("XXX Client error: ", err)
			}
			defer hashRes.Body.Close()
			/* deal Get chunkhash request */
			chunkhash, err := ioutil.ReadAll(hashRes.Body)
			if err != nil {
				fmt.Println("XXX Client error at Read response of chunkhash", err.Error())
				TDFSLogger.Fatal("XXX Client error: ", err)
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

}

// PutFile upload local files from client to DFS
func (client *Client) PutFile(fPath string) {

	chunklist, offsetLast, fileLen := SplitToChunksByName(fPath)

	fileName := path2Name(fPath)
	resFile := client.getReplicaLocations(fileName, fileLen, offsetLast)
	if resFile.Exist {
		fmt.Println("XXX Client error: fileName already in namespace")
		TDFSLogger.Fatal("XXX Client error: fileName already in namespace")
		return
	}
	for i, replicaLocationList := range resFile.File.Chunks {
		PutChunk(fileName, i, chunklist[i], replicaLocationList)
	}
}

// PutChunk push chunks (from the original file) to replicaLocations (get from namenode)
func PutChunk(fileName string, chunkIndex int, chunkBytes []byte, replicaLocationList [REDUNDANCE]ReplicaLocation) {
	replicaLen := len(replicaLocationList)

	for i := 0; i < replicaLen; i++ {
		fmt.Printf("* Putting [Chunk %d] to TDFS [DataNode: %s] at [Replica-%d].\n",
			chunkIndex, replicaLocationList[i].ServerLocation, replicaLocationList[i].ReplicaNum) //  as %s , fName

		/** Create form file **/
		buf := new(bytes.Buffer)
		writer := multipart.NewWriter(buf)
		formFile, err := writer.CreateFormFile("putchunk", fileName)
		if err != nil {
			fmt.Println("XXX Client error at Create form file", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
		}

		// fmt.Println("*** Open source file OK ")

		/** Write to form file **/
		_, err = io.Copy(formFile, bytes.NewReader(chunkBytes))
		if err != nil {
			fmt.Println("XXX Client error at Write to form file", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
		}

		// fmt.Println("*** Write to form file OK ")

		/** Set Params Before Post **/
		params := map[string]string{
			"ReplicaNum": strconv.Itoa(replicaLocationList[i].ReplicaNum), //chunkNum
		}
		for key, val := range params {
			err = writer.WriteField(key, val)
			if err != nil {
				fmt.Println("XXX Client error at Set Params", err.Error())
				TDFSLogger.Fatal("XXX Client error: ", err)
			}
		}
		contentType := writer.FormDataContentType()
		writer.Close() // 发送之前必须调用Close()以写入结尾行

		// fmt.Println("*** Set Params OK ")

		/** Post form file **/
		res, err := http.Post(replicaLocationList[i].ServerLocation+"/putchunk",
			contentType, buf) // /"+strconv.Itoa(chunkNum)
		if err != nil {
			fmt.Println("XXX Client error at Post form file", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
		}
		defer res.Body.Close()

		fmt.Println("** Post form file OK ")

		/** Read response **/
		response, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println("XXX Client error at Read response", err.Error())
			TDFSLogger.Fatal("XXX Client error: ", err)
		}
		fmt.Print("*** DataNoed Response: ", string(response))
	}
}

// getReplicaLocations asks the namenode for the replicaLocation of all the chunks
func (client *Client) getReplicaLocations(fileName string, fileSize int, offsetLast int) *ResFile {
	data := url.Values{
		"fileName":   {fileName},
		"fileSize":   {strconv.Itoa(fileSize)},
		"offsetLast": {strconv.Itoa(offsetLast)},
	}

	response, err := http.PostForm(client.NameNodeAddr+"/getReplicaLocations", data)
	if err != nil {
		fmt.Println("XXX Client error at Get replication location of ", client.NameNodeAddr, ": ", err.Error())
		TDFSLogger.Fatal("XXX Client error: ", err)
	}
	defer response.Body.Close()

	var resFile ResFile
	err = json.NewDecoder(response.Body).Decode(&resFile)
	if err != nil {
		fmt.Println("XXX Client error at decode response to json.", err.Error())
		TDFSLogger.Fatal("XXX Client error: ", err)
	}

	return &resFile
}

func (client *Client) uploadFileByMultipart(fPath string) {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	formFile, err := writer.CreateFormFile("putfile", fPath)
	if err != nil {
		fmt.Println("XXX Client error at Create form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Create form file", err)
	}

	srcFile, err := os.Open(fPath)
	if err != nil {
		fmt.Println("XXX Client error at Open source file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Open source file", err)
	}
	defer srcFile.Close()

	_, err = io.Copy(formFile, srcFile)
	if err != nil {
		fmt.Println("XXX Client error at Write to form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Write to form file", err)
	}

	contentType := writer.FormDataContentType()
	writer.Close() // 发送之前必须调用Close()以写入结尾行
	res, err := http.Post(client.NameNodeAddr+"/putfile", contentType, buf)
	if err != nil {
		fmt.Println("XXX Client error at Post form file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Post form file", err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("XXX Client error at Read response", err.Error())
		TDFSLogger.Fatal("XXX Client error at Read response", err)
	}

	fmt.Println("*** NameNode Response: ", string(content))
}

// SetConfig :set the namenode address for the client
func (client *Client) SetConfig(nnaddr string) {
	client.NameNodeAddr = nnaddr
}

func uploadFileByBody(client *Client, fPath string) {
	file, err := os.Open(fPath)
	if err != nil {
		fmt.Println("XXX Client Fatal error at Open uploadfile", err.Error())
		TDFSLogger.Fatal("XXX Client error at Open uploadfile", err)
	}
	defer file.Close()

	res, err := http.Post(client.NameNodeAddr+"/putfile", "multipart/form-data", file) //
	if err != nil {
		fmt.Println("Client Fatal error at Post", err.Error())
		TDFSLogger.Fatal("XXX Client error at at Post", err)
	}
	defer res.Body.Close()

	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Client Fatal error at Read response", err.Error())
		TDFSLogger.Fatal("XXX Client error at Read response", err)
	}
	fmt.Println(string(content))
}
