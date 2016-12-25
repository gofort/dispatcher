package project_1

import (
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/logger"
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
}

type TaskConfig struct {
	Timeout  int64 // in seconds
	Function interface{}
}

type Worker struct {
	Channel       *amqp.Channel
	StopConsume   chan bool
	TasksFinished chan bool
	Tasks         map[string]TaskConfig
}

func (s *Server) NewWorker(cfg *WorkerConfig) (*Worker, error) {

	// TODO When register new tasks function check their format func (any args of needed types) (result interface{}, err error)

	worker := new(Worker)

	ch, err := s.con.Connection.Channel()
	if err != nil {
		s.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}
	// TODO Need defer ch.Close()

	if err = ch.ExchangeDeclare(
		cfg.Exchange, // name of the exchange
		"direct",     // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		s.log.Errorf("Error during declaring exchange: %s", err)
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

func (w *Worker) ProcessTask(task *Task) {

	// TODO Timeouts

	taskConfig, ok := w.Tasks[task.Name]
	if !ok {
		return
	}

	// TODO Here we can add result backend - task received

	reflectedTaskFunction := reflect.ValueOf(taskConfig.Function)
	reflectedTaskArgs, err := ReflectArgs(task.Args)
	if err != nil {
		// TODO Here we can add result backend - task failed
		return
	}

	// TODO Here we can add result backend - task started

	// Call the task passing in the correct arguments
	results, err := TryCall(reflectedTaskFunction, reflectedTaskArgs)

	if err != nil {
		// TODO Here we can add result backend - task failed
	}

	// TODO Here we can add result backend - task succeed

	return
}

func ReflectArgs(args []TaskArgument) ([]reflect.Value, error) {
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

// TryCall attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
// 1. The reflected function invocation panics (e.g. due to a mismatched
//    argument list).
// 2. The task func itself returns a non-nil error.
func TryCall(f reflect.Value, args []reflect.Value, timeoutSeconds time.Duration) (results []reflect.Value, err error) {

	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = errors.New("Invoking task caused a panic")
			case error:
				err = e
			case string:
				err = errors.New(e)
			}
			// Print stack trace
			fmt.Printf("%s", debug.Stack())
		}
	}()

	timer := time.NewTimer(time.Second * timeoutSeconds)
	resultsChan := make(chan []reflect.Value)

	// TODO Add task UUID to function which we call

	go func() {
		resultsChan <- f.Call(args)
	}()

	select {
	case <-timer.C:
		// TODO Return timeout error
	case results = <-resultsChan:
	}

	// TODO Handle results (even if len == 1, etc.)

	// If an error was returned by the task func, propagate it
	// to the caller via err.
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return results, err
}
