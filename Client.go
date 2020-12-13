package main

import (
	"flag"
	"fmt"

	"github.com/luc/tdfs"
	// "runtime"
	// "sync"
)

func main() {

	var client tdfs.Client
	client.SetConfig("http://namenode:11090")

	filenameOfGet := flag.String("getfile", "unknow", "the filename of the file you want to get") // SmallFile
	filenameOfPut := flag.String("putfile", "unknow", "the filename of the file you want to put") // SmallFile.txt
	filenameOfDel := flag.String("delfile", "unknow", "the filename of the file you want to del")
	filenameOfAppend := flag.Bool("appendfile", false, "the filename of the file you want to del")

	flag.Parse()

	if *filenameOfPut != "unknow" {
		client.PutFile(*filenameOfPut)
		fmt.Println(" -PutFile for ", *filenameOfPut)
	}

	if *filenameOfGet != "unknow" {
		client.GetFile(*filenameOfGet)
		fmt.Println(" -Getfile for ", *filenameOfGet)
	}

	if *filenameOfDel != "unknow" {
		client.DelFile(*filenameOfDel)
		fmt.Println(" -Delfile for ", *filenameOfDel)
	}

	if *filenameOfAppend {
		localFile := flag.Args()[0]
		remoteFile := flag.Args()[1]
		client.AppendFile(localFile, remoteFile)
		fmt.Printf(" -AppendFile from local %s to remote %s \n", localFile, remoteFile)
	}

	// fmt.Println(flag.Args())
	//, "smallfile.txt" /Users/treasersmac/Programming/MilkPrairie/Gou/TinyDFS/
	// client.Test()
}
