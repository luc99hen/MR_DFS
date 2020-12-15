package tdfs

import (
	"log"
)

var (
	TDFSLogger *log.Logger
)

func init() {
	TDFSLogger = LogInit("TDFSLog.txt", "TDFS Log: ")
}

func LogInit(logFilename string, prefix string) (TDFSLogger *log.Logger) {
	CheckPath("./TinyDFS/")
	logFile := OpenFile("./TinyDFS/" + logFilename)
	// fmt.Println(logFile)
	TDFSLogger = log.New(logFile, prefix, log.Ldate|log.Ltime|log.Lshortfile)
	return TDFSLogger
}
