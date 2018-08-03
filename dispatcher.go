package main

import (
	"encoding/json"
	"time"

	"github.com/chrisliu156/sleepwriter-jobs/worker"
	"github.com/chrisliu156/sleepwriter/jobs"
)

type Dispatcher struct {
	JobQueue     chan chan jobs.Job
	NumOfWorkers int
}

func NewDispatcher(workers int) (*Dispatcher, error) {
	dispatcher := new(Dispatcher)
	dispatcher.JobQueue = make(chan chan jobs.Job)
	dispatcher.NumOfWorkers = workers
	return dispatcher, nil
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.NumOfWorkers; i++ {
		sys.log.Infof("Initializing worker %v...", i)
		myworker, _ := worker.NewWorker(i, sys.log, sys.store, d.JobQueue)
		myworker.Run()
	}

	for {
		select {
		case job := <-d.JobQueue:

			// Pop job from queue, if no jobs lets continuosally poll
			var rawPayload []byte
			for {
				jobRaw, popErr := sys.store.PQueuePop(jobQueueName)
				if popErr == nil {
					rawPayload = jobRaw
					break
				} else {
					time.Sleep(2 * time.Second)
				}
			}

			// TODO If we add more types of job will need to put a class type
			var sleepWriteJob jobs.SleepWriterJob
			serializeErr := json.Unmarshal(rawPayload, &sleepWriteJob)
			if serializeErr != nil {
				sys.log.Errorf("Error Serializing Job - %v", serializeErr)
				continue
			}

			// Send the job to the worker
			job <- &sleepWriteJob
		}
	}

}
