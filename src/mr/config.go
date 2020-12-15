package mr

// Mapper is assigned based on DFS chunk locations, MAPPER_NUM cann't be specified here
// const MAPPER_NUM = 3
const REDUCER_NUM = 3

type Worker struct {
	WorkerAddr string
	State      bool
}

type Mapper struct {
	Worker Worker
	Chunks []int
}

type Reducer Worker

type Master struct {
	Mappers  []Mapper
	Reducers [REDUCER_NUM]Reducer
	Addr     string
}
