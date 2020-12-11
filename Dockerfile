# Dockerfile References: https://docs.docker.com/engine/reference/builder/

# Start from the latest golang base image
FROM golang:latest

# Add Maintainer Info
LABEL maintainer="Lu Chen <1813927768@qq.com>"

# Set the Current Working Directory inside the container
WORKDIR /dfs

# Copy go mod and sum files
COPY go.mod go.sum ./

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed

RUN go env -w GO111MODULE=on
RUN go env -w GOPROXY=https://goproxy.io,direct
# ommit errors for `main redeclared ` error
RUN go install; exit 0  

# Expose port to the outside world or use `docker run --expose portnum`
# EXPOSE 11091

# Declare volumes to mount
# VOLUME /TinyDFS/DataNode1  

# Command to run the executable
# ENTRYPOINT ./start.sh ; /bin/bash
