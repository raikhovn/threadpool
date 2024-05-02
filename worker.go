package threadpool

import (
	"fmt"
	"sync"
)

// Interface to be implementated by the function in the Task
type Executable interface {
	Execute(interface{}) interface{}
}

// Task type to be executed
type Task struct {
	Wg       *sync.WaitGroup
	Request  interface{}
	Response interface{}
	Function Executable
}

// Worker type holds the job channel and passed worker threadpool
type Worker struct {
	taskChannel chan *Task
	workerPool  chan chan *Task
	closeHandle chan bool
	id          int
}

// NewWorker creates the new worker
func NewWorker(workerPool chan chan *Task, closeHandle chan bool, id int) *Worker {
	return &Worker{workerPool: workerPool, taskChannel: make(chan *Task), closeHandle: closeHandle, id: id}
}

// Start starts the worker by listening to the job channel
func (w Worker) Start() {
	go func() {
		for {
			// Put the worker to the worker threadpool
			w.workerPool <- w.taskChannel

			select {
			// Wait for the job
			case task := <-w.taskChannel:
				// Got the job
				w.executeTask(task)
				task.Wg.Done()

			case <-w.closeHandle:
				// Exit the go routine when the closeHandle channel is closed
				return
			}
		}
	}()
}

// executeJob executes the job based on the type
func (w Worker) executeTask(task *Task) {
	// Query type for the correct implementation (task.Function needs to be type "Executable")
	switch task.Function.(type) {
	// If type is correct ..execute
	case Executable:
		task.Response = task.Function.Execute(task.Request)
		fmt.Printf("Completed task execution. Thread id: %d\n", w.id)
		break

	}
}
