module github.com/luc/tdfs-mr

go 1.13

require (
	github.com/gin-gonic/gin v1.6.3
	github.com/luc/mr v0.0.0
	github.com/luc/tdfs v0.0.0
	github.com/sqs/goreturns v0.0.0-20181028201513-538ac6014518 // indirect
)

replace (
	github.com/luc/mr => ./src/mr
	github.com/luc/tdfs => ./src/tdfs
)
