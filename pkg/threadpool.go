package pkg

import (
	"fmt"
	"sync"
	//"threadpool"
)

var (
	ErrQueueFull = fmt.Errorf("queue is full, not able add the task")
)

/*
USAGE:



tp := threadpool.NewThreadPool(noOfWorkers := 2, queueSize := 10)

var ret []*threadPool.Task
var ntasks = 20

for i := 0; i < ntasks; i++ {

	var input []int
	input = append(input, i + 1)
	input = append(input, i + 200)

	// Create new Task
	t := Task {
		Request: input,
		Function: NewSampleTask(),
	}

	// Execute
	err := tp.Execute(&t)

	if (err != nil) {
		println("Error occured in threadPool.Execute(): ", err.Error())
	}
	else {
		ret = append(ret, t)
	}

}
// Wait for threads to timeout of finish
tp.Wait()

tp.Close()
*/

//ThreadPool type for holding the workers and handle the job requests
type ThreadPool struct {
	queueSize   int64
	noOfWorkers int
	wg          *sync.WaitGroup
	taskQueue   chan *Task
	workerPool  chan chan *Task
	closeHandle chan bool // Channel used to stop all the workers
}

// NewThreadPool creates thread threadpool
func NewThreadPool(noOfWorkers int, queueSize int64) *ThreadPool {
	threadPool := &ThreadPool{queueSize: queueSize, noOfWorkers: noOfWorkers}
	threadPool.taskQueue = make(chan *Task, queueSize)
	threadPool.workerPool = make(chan chan *Task, noOfWorkers)
	threadPool.closeHandle = make(chan bool)
	threadPool.wg = &sync.WaitGroup{}
	threadPool.createPool()

	return threadPool
}

func (t *ThreadPool) submitTask(task *Task) error {
	// Add the task to the job queue
	if len(t.taskQueue) == int(t.queueSize) {
		return ErrQueueFull
	}
	// Add counter for WaitGroup mutex
	t.wg.Add(1)
	task.Wg = t.wg
	t.taskQueue <- task
	return nil
}

// Execute submits the job to available worker
func (t *ThreadPool) Execute(task *Task) error {
	return t.submitTask(task)
}

// Wait for completion and results
func (t *ThreadPool) Wait() {
	// Each thread should have a way to time out
	t.wg.Wait()
}

// Close will close the threadpool
// It sends the stop signal to all the worker that are running
//TODO: need to check the existing /running task before closing the threadpool
func (t *ThreadPool) Close() {
	close(t.closeHandle) // Stops all the routines
	close(t.workerPool)  // Closes the Job threadpool
	close(t.taskQueue)   // Closes the job Queue
}

// createPool creates the workers and start listening on the jobQueue
func (t *ThreadPool) createPool() {
	for i := 0; i < t.noOfWorkers; i++ {
		worker := NewWorker(t.workerPool, t.closeHandle, i+1)
		worker.Start()
	}

	go t.dispatch()

}

// dispatch listens to the jobqueue and handles the jobs to the workers
func (t *ThreadPool) dispatch() {
	for {
		select {

		case task := <-t.taskQueue:
			// Got job
			func(task *Task) {
				//Find a worker for the job
				taskChannel := <-t.workerPool
				//Submit job to the worker
				taskChannel <- task
			}(task)

		case <-t.closeHandle:
			// Close thread threadpool
			return
		}
	}
}
