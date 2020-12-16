package main

import "github.com/luc/mr"

const WORKER_ADDR = "http://localhost:11101"

func main() {
	var worker mr.Worker

	worker.WorkerAddr = WORKER_ADDR
	worker.Run()
}
