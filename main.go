package main

import (
	"os"
	"strconv"

	"github.com/chrisliu156/sleepwriter/store"
	"github.com/chrisliu156/sleepwriter/utils"
	"github.com/sirupsen/logrus"
)

var (
	logLevel     = utils.GetEnv("log_level", "DEBUG")
	jobQueueName = utils.GetEnv("job_queue_name", "job_queue")
	numOfWorkers = utils.GetEnv("num_of_workers", "10")
)

type System struct {
	store store.Store
	log   *logrus.Logger
}

var sys *System

func init() {
	sys = &System{}
	sys.log = logrus.New()
	sys.log.Out = os.Stdout

	switch logLevel {
	case "DEBUG":
		sys.log.Level = logrus.DebugLevel
	case "INFO":
		sys.log.Level = logrus.InfoLevel
	}

	pool, poolErr := store.NewStore()
	if poolErr != nil {
		sys.log.Fatalf("Initialization - Error - Store connection error - %v", poolErr)
	}
	sys.store = *pool
}

func main() {
	sys.log.Infoln("Initialize worker service...")
	workersnum, _ := strconv.Atoi(numOfWorkers)
	dispatcher, _ := NewDispatcher(workersnum)
	dispatcher.Run()
}
