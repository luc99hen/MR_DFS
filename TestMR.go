package main

import (
	"github.com/luc/mr"
	"github.com/luc/tdfs"
)

func main() {
	contents := tdfs.ReadFileByBytes("./data/call.txt")
	mr.Task2Map("./data/call.txt", string(contents))
	// fmt.Println(mr.Task2Reduce("./data/call.txt", []string{"1", "2", "4", "4", "4"}))
}
