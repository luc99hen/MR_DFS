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

// PutFile :upload local files from client to DFS
// func (client *Client) PutFile(fPath string) {
// 	fmt.Println("****************************************")
// 	fmt.Printf("*** Putting ${GOPATH}/%s to TDFS [NameNode: %s] )\n", fPath, client.NameNodeAddr) //  as %s , fName

// 	client.uploadFileByMultipart(fPath)

// 	fmt.Println("****************************************")
// }

// GetFile :download file from DFS
func (client *Client) GetFile(fName string) { //, fName string
	fmt.Println("****************************************")
	fmt.Printf("*** Getting from TDFS [NameNode: %s] to ${GOPATH}/%s )\n", client.NameNodeAddr, fName) //  as %s , fName

	response, err := http.Get(client.NameNodeAddr + "/getfile/" + fName)
	if err != nil {
		fmt.Println("XXX Client error at Get file", err.Error())
		TDFSLogger.Fatal("XXX Client error at Get file", err)
	}

	defer response.Body.Close()

	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("XXX Client error at read response data", err.Error())
		TDFSLogger.Fatal("XXX Client error at read response data", err)
	}

	err = ioutil.WriteFile("local-"+fName, bytes, 0666)
	if err != nil {
		fmt.Println("XXX Client error at store file", err.Error())
		TDFSLogger.Fatal("XXX Client error at store file", err)
	}

	fmt.Print("*** NameNode Response: ", string(bytes))
	fmt.Println("****************************************")
}

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

	// // Display Results
	// fmt.Println("response Status : ", resp.Status)
	// fmt.Println("response Headers : ", resp.Header)
	// fmt.Println("response Body : ", string(respBody))

	// // response, err := http.Delete(client.NameNodeAddr + "/delfile/" + fName) //Get
	// if err != nil {
	// 	fmt.Println("XXX Client error at del file", err.Error())
	// 	TDFSLogger.Fatal("XXX Client error at del file",err)
	// }

	// defer response.Body.Close()

	// bytes, err := ioutil.ReadAll(response.Body)
	// if err != nil {
	// 	fmt.Println("XXX Client error at read response data", err.Error())
	// 	TDFSLogger.Fatal("XXX Client error at read response data",err)
	// }

	fmt.Print("*** NameNode Response: ", string(bytes))
	fmt.Println("****************************************")
}

func (client *Client) Test() {
	// _, err := http.Get(client.NameNodeAddr + "/test")
	// if err != nil {
	// 	fmt.Println("Client error at Get test", err.Error())
	// }
	// client.PutFile("./data/AFile.txt")
}

// PutFile upload local files from client to DFS
func (client *Client) PutFile(fPath string) {

	chunklist, offsetLast, fileLen := SplitToChunksByName(fPath)

	fileName := path2Name(fPath)
	fileChunks := client.getReplicaLocations(fileName, fileLen, offsetLast)
	for i, replicaLocationList := range fileChunks {
		PutChunk(fileName, i, chunklist[i], replicaLocationList)
	}
}

// PutChunk push chunks (from the original file) to replicaLocations (get from namenode)
func PutChunk(fileName string, chunkIndex int, chunkBytes []byte, replicaLocationList [REDUNDANCE]ReplicaLocation) {
	replicaLen := len(replicaLocationList)

	for i := 0; i < replicaLen; i++ {
		fmt.Printf("* Putting [Chunk %d] to TDFS [DataNode: %s] at [Replica-%d].\n",
			chunkIndex, replicaLocationList[0].ServerLocation, replicaLocationList[0].ReplicaNum) //  as %s , fName

		/** Create form file **/
		buf := new(bytes.Buffer)
		writer := multipart.NewWriter(buf)
		formFile, err := writer.CreateFormFile("putchunk", fileName)
		if err != nil {
			fmt.Println("XXX NameNode error at Create form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		// fmt.Println("*** Open source file OK ")

		/** Write to form file **/
		_, err = io.Copy(formFile, bytes.NewReader(chunkBytes))
		if err != nil {
			fmt.Println("XXX NameNode error at Write to form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}

		// fmt.Println("*** Write to form file OK ")

		/** Set Params Before Post **/
		params := map[string]string{
			"ReplicaNum": strconv.Itoa(replicaLocationList[i].ReplicaNum), //chunkNum
		}
		for key, val := range params {
			err = writer.WriteField(key, val)
			if err != nil {
				fmt.Println("XXX NameNode error at Set Params", err.Error())
				TDFSLogger.Fatal("XXX NameNode error: ", err)
			}
		}
		contentType := writer.FormDataContentType()
		writer.Close() // 发送之前必须调用Close()以写入结尾行

		// fmt.Println("*** Set Params OK ")

		/** Post form file **/
		res, err := http.Post(replicaLocationList[i].ServerLocation+"/putchunk",
			contentType, buf) // /"+strconv.Itoa(chunkNum)
		if err != nil {
			fmt.Println("XXX NameNode error at Post form file", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		defer res.Body.Close()

		fmt.Println("** Post form file OK ")

		/** Read response **/
		response, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Println("XXX NameNode error at Read response", err.Error())
			TDFSLogger.Fatal("XXX NameNode error: ", err)
		}
		fmt.Print("*** DataNoed Response: ", string(response))
	}
}

// getReplicaLocations asks the namenode for the replicaLocation of all the chunks
func (client *Client) getReplicaLocations(fileName string, fileSize int, offsetLast int) []FileChunk {
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

	var f File
	err = json.NewDecoder(response.Body).Decode(&f)
	if err != nil {
		fmt.Println("XXX Client error at decode response to json.", err.Error())
		TDFSLogger.Fatal("XXX Client error: ", err)
	}

	return f.Chunks

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

func RequestInfo(fileName string, fileBytes int) []ReplicaLocation {
	/* POST and Wait */
	replicaLocationList := []ReplicaLocation{
		{"http://localhost:11091", 3},
		{"http://localhost:11092", 5},
	}
	return replicaLocationList
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
