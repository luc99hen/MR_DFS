package tdfs

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func splitToFileAndStore(fileName string, storeFile string) (chunkLen int, offsetLast int) {
	data := ReadFileByBytes(fileName)
	var i int = 0
	for i < len(data)/CHUNK_SIZE {
		FastWrite(storeFile+strconv.Itoa(i), data[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
		i++
	}
	FastWrite(storeFile+strconv.Itoa(i), data[i*CHUNK_SIZE:])
	chunkLen = i
	offsetLast = len(data) - i*CHUNK_SIZE

	return chunkLen + 1, offsetLast
}

func FastWrite(fileName string, data []byte) {
	err := ioutil.WriteFile(fileName, data, 0666)
	if err != nil {
		fmt.Println("XXX Utils error at FastWrite", err.Error())
		TDFSLogger.Panic("XXX Utils error at FastWrite", err)
	}
}

func CreateFile(fileName string) (newFile *os.File) {
	defer newFile.Close()
	newFile, err := os.Create(fileName)
	if err != nil {
		fmt.Println("XXX Utils error at CreateFile", err.Error())
		TDFSLogger.Panic("XXX Utils error at CreateFile", err)
	}
	// TDFSLogger.Println(newFile)
	return newFile
}

func showFileInfo(fileName string) {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		TDFSLogger.Panic(err)
	}
	fmt.Println("File name:", fileInfo.Name())
	fmt.Println("Size in bytes:", fileInfo.Size())
	fmt.Println("Permissions:", fileInfo.Mode())
	fmt.Println("Last modified:", fileInfo.ModTime())
	fmt.Println("Is Directory: ", fileInfo.IsDir())
	fmt.Printf("System interface type: %T\n", fileInfo.Sys())
	fmt.Printf("System info: %+v\n\n", fileInfo.Sys())
}

func DeleteFile(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		fmt.Println("XXX Utils error at DeleteFile ", fileName, ":", err.Error())
		TDFSLogger.Panic("XXX Utils error at DeleteFile ", err)
	}
}

func CleanFile(fileName string) {
	DeleteFile(fileName)
	CreateFile(fileName)
}

func OpenFile(fileName string) (file *os.File) {
	defer file.Close()
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("XXX Utils error at OpenFile :", err.Error())
		TDFSLogger.Panic("XXX Utils error at OpenFile :", err)
	}
	return file
}

func copyFile(oriFilename string, newFilename string) {
	oriFile, err := os.Open(oriFilename)
	if err != nil {
		fmt.Println("XXX Utils error at copyFile(Open) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at copyFile(Open) :", err)
	}
	defer oriFile.Close()

	newFile, err := os.Create(newFilename)
	if err != nil {
		fmt.Println("XXX Utils error at copyFile(Create) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at copyFile(Create) :", err)
	}
	defer newFile.Close()

	bytesWritten, err := io.Copy(newFile, oriFile)
	if err != nil {
		fmt.Println("XXX Utils error at copyFile(Copy) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at copyFile(Copy) :", err)
	}
	TDFSLogger.Printf("Copied %d bytes.", bytesWritten)

	err = newFile.Sync()
	if err != nil {
		fmt.Println("XXX Utils error at copyFile(Sync) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at copyFile(Sync) :", err)
	}
}

func readFileLimitedBytes(fileName string, limit int64) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("XXX Utils error at readFileLimitedBytes(Open) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at readFileLimitedBytes(Open) :", err)
	}
	byteSlice := make([]byte, limit)
	numBytesRead, err := io.ReadFull(file, byteSlice)
	if err != nil {
		fmt.Println("XXX Utils error at readFileLimitedBytes(ReadFull) :", err.Error())
		TDFSLogger.Panic("XXX Utils error at readFileLimitedBytes(ReadFull) :", err)
	}
	fmt.Printf("Number of bytes read: %d\n", numBytesRead)
	fmt.Printf("Data read: \n%s", byteSlice)
	fmt.Println()
}

func WriteFile(fileName string, lines []string) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		_, err = fmt.Fprintln(w, line)
		if err != nil {
			return err
		}
	}
	return w.Flush()
}

// ReadFileByBytes parse the file by fileName to bytes
func ReadFileByBytes(fileName string) []byte {
	file, err := os.Open(fileName)
	defer func() {
		file.Close()
	}()
	if err != nil {
		fmt.Println("XXX Utils error at readFileByBytes(open): ", err.Error())
		TDFSLogger.Panic("XXX Utils error at readFileByBytes(open): ", err)
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("XXX Utils error at readFileByBytes(ReadAll): ", err.Error())
		TDFSLogger.Panic("XXX Utils error at readFileByBytes(ReadAll): ", err)
	}
	return data
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// CheckPath checks if path exist, if not create a new one
func CheckPath(path string) {
	exist, err := PathExists(path)
	if err != nil {
		fmt.Println("XXX Utils error at PathExist in CheckPath() ", err.Error())
		TDFSLogger.Panic("XXX Utils error: ", err)
	}
	if !exist {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			fmt.Println("XXX Utils error at MkdirAll in CheckPath() ", err.Error())
			TDFSLogger.Panic("XXX Utils error: ", err)
		}
	}
}

func SplitToChunksByName(fPath string) (chunklist []ChunkUnit, offsetLast int, fileLen int) {
	data := ReadFileByBytes(fPath)
	var i int = 0
	fileLen = len(data)
	for i < fileLen/CHUNK_SIZE {
		chunklist = append(chunklist, data[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE])
		i++
	}
	chunklist = append(chunklist, data[i*CHUNK_SIZE:])
	offsetLast = fileLen - i*CHUNK_SIZE
	return chunklist, offsetLast, fileLen
}

func Path2Name(fPath string) (fileName string) {
	tmp := strings.Split(fPath, "/") // in case a full path passed
	return tmp[len(tmp)-1]
}

func GetHashStr(bytes []byte) (hashStr string) {
	hash := sha256.New()
	hash.Write(bytes)
	hashStr = hex.EncodeToString(hash.Sum(nil))
	return hashStr
}

func GetHashInt(bytes []byte) uint32 {
	h := fnv.New32a()
	h.Write(bytes)
	return h.Sum32()
}

func getChunkLength(fileSize int) int {
	if fileSize%CHUNK_SIZE == 0 {
		return fileSize / CHUNK_SIZE
	}
	return fileSize/CHUNK_SIZE + 1
}

func updateDataNodeMetadata(datanode *DataNode) {
	n := datanode.StorageAvail
	datanode.ChunkAvail[0] = datanode.ChunkAvail[n-1]
	datanode.ChunkAvail = datanode.ChunkAvail[0 : n-1]
	datanode.StorageAvail--
}
