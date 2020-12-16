package main

import "github.com/luc/mr"

const MASTER_ADDR = "11100"

func main() {
	var master mr.Master

	master.SetConfig(MASTER_ADDR)
	master.Run()
}
