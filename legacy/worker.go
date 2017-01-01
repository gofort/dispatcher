package dispatcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofort/dispatcher/utils"
	"github.com/streadway/amqp"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

type WorkerConfig struct {
	Limit        int
	Exchange     string
	DefaultQueue string
	BindingKeys  []string
	Name         string
}

type TaskConfig struct {
	TimeoutSeconds int64
	Function       interface{}
}

type Worker struct {
	channel         *amqp.Channel
	name            string
	log             Log
	stopConsume     chan bool
	tasksFinished   chan bool
	connect         chan bool
	tasks           map[string]TaskConfig
	TasksInProgress TasksInProgress
}

type TasksInProgress struct {
	sync.Mutex
	counter int
}

func (t *TasksInProgress) inc() {
	t.Lock()
	t.counter++
	t.Unlock()
}

func (t *TasksInProgress) dec() {
	t.Lock()
	t.counter--
	t.Unlock()
}

func (t *TasksInProgress) Get() int {
	t.Lock()
	defer t.Unlock()
	return t.counter
}

func (s *Server) NewWorker(cfg *WorkerConfig, tasks map[string]TaskConfig) (*Worker, error) {

	worker := new(Worker)
	worker.log = s.log

	worker.name = cfg.Name

	worker.tasksFinished = make(chan bool)

	ch, err := s.con.Connection.Channel()
	if err != nil {
		s.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}
	defer ch.Close()

	err = declareExchange(ch, cfg.Exchange)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	err = declareQueue(ch, cfg.DefaultQueue)
	if err != nil {
		s.log.Errorf("Error during declaring queue: %s", err)
		return nil, err
	}

	for _, k := range cfg.BindingKeys {

		err = queueBind(ch, cfg.Exchange, cfg.DefaultQueue, k)
		if err != nil {
			s.log.Errorf("Error during binding queue: %s", err)
			return nil, err
		}

	}

	go func() {

		for {

			select {

			case <-worker.connect:

				worker.channel.Close()

				delivery, err := worker.initWorker(s.con.Connection, cfg.DefaultQueue, cfg.Name, cfg.Limit)
				if err != nil {
					s.log.Error(err)
					return nil, err
				}

			}

		}

	}()

	worker.connect <- true

	return worker, nil

}

func (w *Worker) Stop() {

	w.stopConsume <- true

	go func() {
		for range time.Tick(time.Second * 1) {
			t := w.TasksInProgress.Get()
			if t == 0 {
				w.log.Infof("Worker %s has finished all tasks", w.name)
				w.tasksFinished <- true
				break
			}
			w.log.Infof("Worker %s has %d unfinished tasks", w.name, t)
		}
	}()

}

func (w *Worker) initWorker(con *amqp.Connection, queue string, workerName string, limit int) (<-chan amqp.Delivery, error) {

	var err error

	w.channel, err = con.Channel()
	if err != nil {
		w.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}
	//defer ch.Close()
	// TODO Close??

	if err := w.channel.Qos(
		limit, // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		w.log.Error(err)
		return nil, err
	}

	deliveries, err := w.channel.Consume(
		queue,      // queue
		workerName, // consumer tag
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		w.log.Error(err)
		return nil, err
	}

	w.stopConsume = make(chan bool)

}

func (w *Worker) consume(deliveries <-chan amqp.Delivery) error {

	errorsChan := make(chan error)

	for {
		select {
		case err := <-errorsChan:

			// TODO Try to reinit worker?

		case d := <-deliveries:

			go func() {
				w.TasksInProgress.inc()
				w.consumeOne(d, errorsChan)
				w.TasksInProgress.dec()
			}()

		case <-w.stopConsume:

			// TODO close(deliveries) ?
			// TODO close channel?

			return nil
		}
	}
}

func (w *Worker) consumeOne(d amqp.Delivery, errorsChan chan error) {

	if len(d.Body) == 0 {

		if err := d.Nack(false, false); err != nil {
			w.log.Error(err)
			errorsChan <- err
		}

		w.log.Error(errors.New("Empty task received"))
		return
	}

	var task Task
	if err := json.Unmarshal(d.Body, &task); err != nil {

		if er := d.Nack(false, false); er != nil {
			w.log.Error(er)
			errorsChan <- er
		}

		w.log.Error(errors.New("Can't unmarshal received task"))
		return
	}

	taskConfig, ok := w.tasks[task.Name]
	if !ok {
		if er := d.Nack(false, true); er != nil {
			w.log.Error(er)
			errorsChan <- er
		}
		return
	}

	reflectedTaskFunction := reflect.ValueOf(taskConfig.Function)
	reflectedTaskArgs, err := reflectArgs(task.Args)
	if err != nil {
		w.log.Error(err)
		return
	}

	tryCall(reflectedTaskFunction, reflectedTaskArgs, taskConfig.TimeoutSeconds)

	if err := d.Ack(false); err != nil {
		w.log.Error(err)
		errorsChan <- err
	}

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
		// TODO Log about timeout?
	case <-resultsChan:
	}

	return
}
