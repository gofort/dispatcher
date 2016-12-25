package dispatcher

import (
	"fmt"
	"github.com/gofort/dispatcher/utils"
	"github.com/streadway/amqp"
	"reflect"
	"runtime/debug"
	"time"
)

type WorkerConfig struct {
	Limit        int    // Parallel tasks limit
	Exchange     string // dispatcher
	DefaultQueue string // scalet_backup, scalet_management, scalet_create
	BindingKey   string // If no routing key in task - this will be used, it is like default routing key for all tasks
	Name         string
}

type TaskConfig struct {
	TimeoutSeconds int64 // in seconds
	Function       interface{}
}

type Worker struct {
	Channel       *amqp.Channel
	log           Log
	StopConsume   chan bool
	TasksFinished chan bool
	Tasks         map[string]TaskConfig
}

func (s *Server) NewWorker(cfg *WorkerConfig, tasks map[string]TaskConfig) (*Worker, error) {

	worker := new(Worker)
	worker.log = s.log

	ch, err := s.con.Connection.Channel()
	if err != nil {
		s.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}

	err = declareExchange(ch, cfg.Exchange)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	queue, err := ch.QueueDeclare(
		cfg.DefaultQueue, // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		s.log.Errorf("Error during declaring queue: %s", err)
		return nil, err
	}

	if err := ch.QueueBind(
		queue.Name,     // name of the queue
		cfg.BindingKey, // binding key
		cfg.Exchange,   // source exchange
		false,          // noWait
		amqp.Table(map[string]interface{}{}), // TODO arguments, not implemented
	); err != nil {
		s.log.Errorf("Error during binding queue: %s", err)
		return nil, err
	}

	if err = ch.Qos(
		cfg.Limit, // prefetch count
		0,         // prefetch size
		false,     // global
	); err != nil {
		return nil, err
	}

	worker.Channel = ch

	return worker, nil

}

// TODO Stop Worker

func (w *Worker) startConsume(ch *amqp.Channel, queue string, workerName string) (<-chan amqp.Delivery, error) {

	return ch.Consume(
		queue,      // queue
		workerName, // consumer tag
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // arguments
	)

}

func (w *Worker) processTask(task *Task) {

	taskConfig, ok := w.Tasks[task.Name]
	if !ok {
		return
	}

	reflectedTaskFunction := reflect.ValueOf(taskConfig.Function)
	reflectedTaskArgs, err := reflectArgs(task.Args)
	if err != nil {
		w.log.Error(err)
		return
	}

	tryCall(reflectedTaskFunction, reflectedTaskArgs, taskConfig.TimeoutSeconds)

	return
}

func reflectArgs(args []TaskArgument) ([]reflect.Value, error) {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := utils.ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return nil, err
		}
		argValues[i] = argValue
	}

	return argValues, nil
}

func tryCall(f reflect.Value, args []reflect.Value, timeoutSeconds int64) {

	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("%s", debug.Stack())
		}
	}()

	// TODO Add task UUID to function which we call

	if timeoutSeconds == 0 {
		f.Call(args)
		return
	}

	timer := time.NewTimer(time.Second * time.Duration(timeoutSeconds))
	resultsChan := make(chan []reflect.Value)

	go func() {
		resultsChan <- f.Call(args)
	}()

	select {
	case <-timer.C:
	case <-resultsChan:
	}

	return
}
