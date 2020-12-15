package mr

import (
	"strings"

	"github.com/gin-gonic/gin"
)

func (worker *Worker) Run() {
	router := gin.Default()
	router.MaxMultipartMemory = 1024 << 20

	router.POST("/doMap", func(c *gin.Context) {
		// get chunk id from paras
		// call user-defined mapper function, write output to mout-i
		// append mout-i to rin-i in dfs
	})

	router.POST("/doReduce", func(c *gin.Context) {
		// read rin-i from dfs
		// call user-defined reducer function, write output to rout-i
		// append rout-i to mrout
	})

	router.Run(":" + strings.Split(worker.WorkerAddr, ":")[0])
}
