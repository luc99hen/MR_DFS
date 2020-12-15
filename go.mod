module github.com/luc/tdfs-nodes

go 1.13

require (
	github.com/gin-gonic/gin v1.6.3
	github.com/luc/tdfs v0.0.0
	github.com/sqs/goreturns v0.0.0-20181028201513-538ac6014518 // indirect
)

replace github.com/luc/tdfs => ./src/tdfs
