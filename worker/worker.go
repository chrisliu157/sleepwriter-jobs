package worker

import (
	"encoding/json"

	"github.com/chrisliu156/sleepwriter/jobs"
	"github.com/chrisliu156/sleepwriter/store"
	"github.com/sirupsen/logrus"
)

type Worker struct {
	Id       int
	Job      chan jobs.Job
	JobQueue chan chan jobs.Job
	log      *logrus.Logger
	store    store.Store
}

func NewWorker(id int, log *logrus.Logger, store store.Store, jobQueue chan chan jobs.Job) (*Worker, error) {
	myworker := new(Worker)
	myworker.Id = id
	myworker.log = log
	myworker.Job = make(chan jobs.Job)
	myworker.JobQueue = jobQueue
	myworker.store = store
	return myworker, nil
}

func (w *Worker) Run() {
	go func() {
		for {
			// Telling the dispatcher we are ready for the next job
			w.JobQueue <- w.Job

			select {
			case job := <-w.Job:
				w.log.Debugf("Worker: %v - Working on Job Id: %v", w.Id, job.GetId())

				// Set the job status to In Progress
				w.UpdateJobStatus(job, jobs.INPROGRESS)

				processErr := job.Process(w.store)
				if processErr != nil {
					w.log.Debugf("Worker: %v - Error - Job Id: %v  - %v", w.Id, job.GetId(), processErr)
					continue
				}

				// Set the job status to Completed
				w.UpdateJobStatus(job, jobs.SUCCESS)
				w.log.Debugf("Worker: %v - Completed Job Id: %v", w.Id, job.GetId())
			}
		}
	}()
}

func (w Worker) UpdateJobStatus(job jobs.Job, status string) {
	job.SetStatus(status)
	payload, _ := json.Marshal(job)
	w.store.Set(job.GetId(), payload)
}
