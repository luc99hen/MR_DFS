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
	fmt.Printf("*** DelFile from TDFS [NameNode: %s] of /%s )\n", client.NameNodeAddr, fName)

	// Create client
	c := &http.Client{}
	// Create request
	req, err := http.NewRequest("DELETE", client.NameNodeAddr+"/delfile/"+fName, nil)
	if err != nil {
		fmt.Println("XXX Client error at del file(NewRequest):", err.Error())
		TDFSLogger.Panic("XXX Client error at del file(NewRequest):", err)
	}
	// Fetch Request
	response, err := c.Do(req)
	if err != nil || response.StatusCode != http.StatusOK {
		fmt.Println("XXX Client error at del file(Do)", err)
		TDFSLogger.Panic("XXX Client error at del file(Do):", err)
	}
	defer response.Body.Close()
	// Read Response Body
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("XXX Client error at read response data", err.Error())
		TDFSLogger.Panic("XXX Client error at read response data", err)
	}

	fmt.Print("*** NameNode Response: ", string(bytes))
	fmt.Println("****************************************")
}

func (client *Client) Test() {
	// _, err := http.Get(client.NameNodeAddr + "/test")
	// if err != nil {
	// 	fmt.Println("Client error at Get test", err.Error())
	// }
	client.PutFile("./data/AFile.txt")
	client.GetFile("AFile.txt")
	client.AppendFile("./data/BFile.txt", "AFile.txt")
	client.GetFile("AFile.txt")
}

// GetFile download file from DFS
func (client *Client) GetFile(fileName string) []byte {
	fmt.Println("****************************************")
	fmt.Printf("*** GetFile from TDFS [NameNode: %s] of /%s )\n", client.NameNodeAddr, fileName)

	dummyParas := 0
	resFile := client.getReplicaLocations(fileName, dummyParas, dummyParas, "get")

	if !resFile.Exist {
		fmt.Println("XXX Client error at GetFile, file not exist")
		TDFSLogger.Panic("XXX Client error at GetFile, file not exist")
		return nil
	}

	GetChunks(&resFile.File, fileName)
	bytes := AssembleChunks(&resFile.File, fileName)

	fmt.Println("****************************************")
	return bytes
}

func AssembleChunks(file *File, fileName string) []byte {
	fmt.Println("*** AssembleFile of ", fileName)

	chunkLength := getChunkLength(file.Size)
	filedata := make([][]byte, chunkLength)
	tmpChunkPath := "./" + GetHashStr([]byte(fileName))
	for i := 0; i < chunkLength; i++ {
		b := ReadFileByBytes(tmpChunkPath + "/chunk-" + strconv.Itoa(i))
		filedata[i] = make([]byte, CHUNK_SIZE)
		filedata[i] = b
	}

	// remove tmp chunk file created by GetChunks()
	os.RemoveAll(tmpChunkPath)

	fdata := bytes.Join(filedata, nil)
	FastWrite("./local-"+fileName, fdata)
	fmt.Print("*** Assemble Complete: ", string(fdata))
	return fdata
}

// GetChunks pulls all the file chunk from datanode and store it in a temporary directory in clients
func GetChunks(file *File, fileName string) { //ChunkUnit chunkbytes []byte

	// prepare for the path store tmp chunks get from datanode
	tmpChunkPath := "./" + GetHashStr([]byte(fileName))
	CheckPath(tmpChunkPath)

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
				TDFSLogger.Panic("XXX Client error: ", err)
			}
			defer dataRes.Body.Close()

			if dataRes.StatusCode != http.StatusOK {
				fmt.Println("X=X the first replica of chunk-", num, "'s hash(checksum) is WRONG, continue to request anothor replica...")
				continue
			}

			/* deal response of Get */
			chunkbytes, err := ioutil.ReadAll(dataRes.Body)
			if err != nil {
				fmt.Println("XXX Client error at ReadAll response of chunk", err.Error())
				TDFSLogger.Panic("XXX Client error: ", err)
			}
			fmt.Println("** DataNode Response of Get chunk-", num, ": ", string(chunkbytes))
			/* store chunkdata at nn local */

			FastWrite(tmpChunkPath+"/chunk-"+strconv.Itoa(num), chunkbytes)
		}
	}

}

// AppendFile append localfile to the remotefile in DFS
func (client *Client) AppendFile(localFile string, remoteFile string) {
	fmt.Println("****************************************")
	fmt.Printf("*** AppendFile %s to TDFS [NameNode: %s] of %s )\n", localFile, client.NameNodeAddr, remoteFile)

	client.uploadFile(localFile, remoteFile, "append")

	fmt.Println("****************************************")
}

// PutFile upload local files from client to DFS
func (client *Client) PutFile(localFile string) []FileChunk {
	fmt.Println("****************************************")
	fmt.Printf("*** PutFile %s to TDFS [NameNode: %s] )\n", localFile, client.NameNodeAddr)

	chunks := client.uploadFile(localFile, localFile, "put")

	fmt.Println("****************************************")
	return chunks
}

func (client *Client) uploadFile(localFile string, remoteFile, mode string) []FileChunk {

	chunklist, offsetLast, fileLen := SplitToChunksByName(localFile)

	// incase localFile is a path
	fileName := Path2Name(remoteFile)
	resFile := client.getReplicaLocations(fileName, fileLen, offsetLast, mode)
	if resFile.Exist && mode == "put" {
		fmt.Println("XXX Client error: file already exist in namespace")
		TDFSLogger.Panic("XXX Client error: file already exist in namespace")
		return nil
	}
	for i, replicaLocationList := range resFile.File.Chunks {
		PutChunk(fileName, i, chunklist[i], replicaLocationList)
	}
	return resFile.File.Chunks
}

// PutChunk push chunks (from the original file) to replicaLocations (get from namenode)
func PutChunk(fileName string, chunkIndex int, chunkBytes []byte, replicaLocationList FileChunk) {
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
			TDFSLogger.Panic("XXX Client error: ", err)
		}

		// fmt.Println("*** Open source file OK ")

		/** Write to form file **/
		_, err = io.Copy(formFile, bytes.NewReader(chunkBytes))
		if err != nil {
			fmt.Println("XXX Client error at Write to form file", err.Error())
			TDFSLogger.Panic("XXX Client error: ", err)
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
				TDFSLogger.Panic("XXX Client error: ", err)
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
			TDFSLogger.Panic("XXX Client error: ", err)
		}
		defer res.Body.Close()

		fmt.Println("** Post form file OK ")

		/** Read response **/
		response, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println("XXX Client error at Read response", err.Error())
			TDFSLogger.Panic("XXX Client error: ", err)
		}
		fmt.Print("*** DataNoed Response: ", string(response))
	}
}

// getReplicaLocations asks the namenode for the replicaLocation of all the chunks
func (client *Client) getReplicaLocations(fileName string, fileSize int, offsetLast int, from string) *ResFile {
	data := url.Values{
		"fileName":   {fileName},
		"fileSize":   {strconv.Itoa(fileSize)},
		"offsetLast": {strconv.Itoa(offsetLast)},
		"mode":       {from}, // record where is this request from (put ,get or append)
	}

	response, err := http.PostForm(client.NameNodeAddr+"/getReplicaLocations", data)
	if err != nil || response.StatusCode != http.StatusOK {
		fmt.Println("XXX Client error at Get replication location of ", client.NameNodeAddr, err)
		TDFSLogger.Panic("XXX Client error at Get replication location", err)
	}
	defer response.Body.Close()

	var resFile ResFile
	err = json.NewDecoder(response.Body).Decode(&resFile)
	if err != nil {
		fmt.Println("XXX Client error at decode response to json.", err.Error())
		TDFSLogger.Panic("XXX Client error: ", err)
	}

	return &resFile
}

// SetConfig :set the namenode address for the client
func (client *Client) SetConfig(nnaddr string) {
	client.NameNodeAddr = nnaddr
}
