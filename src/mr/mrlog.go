package mr

import (
	"fmt"
	"log"

	"github.com/luc/tdfs"
)

var (
	MRLogger *log.Logger
)

func init() {
	MRLogger = LogInit("MRLog.txt", "MR Log: ")
}

func LogInit(logFilename string, prefix string) (TDFSLogger *log.Logger) {
	tdfs.CheckPath("./TinyDFS/")
	logFile := tdfs.OpenFile("./TinyDFS/" + logFilename)
	// fmt.Println(logFile)
	TDFSLogger = log.New(logFile, prefix, log.Ldate|log.Ltime|log.Lshortfile)
	return TDFSLogger
}

func MyFatal(v ...interface{}) {
	fmt.Println(v...)
	MRLogger.Fatal(v...)
}
