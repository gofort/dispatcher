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
	Limit       int
	Exchange    string
	Queue       string
	BindingKeys []string
	Name        string
}

type TaskConfig struct {
	TimeoutSeconds int64
	Function       interface{}
}

type Worker struct {
	ch              *amqp.Channel
	name            string
	log             Log
	tasksInProgress *sync.WaitGroup
	tasks           map[string]TaskConfig
	limit           int
	queue           string
	deliveries      <-chan amqp.Delivery
	stopConsume     chan bool
}

func (s *Server) NewWorker(cfg *WorkerConfig, tasks map[string]TaskConfig) (*Worker, error) {

	w := &Worker{
		name:            cfg.Name,
		log:             s.log,
		tasks:           tasks,
		limit:           cfg.Limit,
		queue:           cfg.Queue,
		stopConsume:     make(chan bool, 1),
		tasksInProgress: new(sync.WaitGroup),
	}

	var err error

	w.ch, err = s.con.Connection.Channel()
	if err != nil {
		s.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}

	err = declareExchange(w.ch, cfg.Exchange)
	if err != nil {
		s.log.Error(err)
		return nil, err
	}

	err = declareQueue(w.ch, cfg.Queue)
	if err != nil {
		s.log.Errorf("Error during declaring queue: %s", err)
		return nil, err
	}

	for _, k := range cfg.BindingKeys {

		err = queueBind(w.ch, cfg.Exchange, cfg.Queue, k)
		if err != nil {
			s.log.Errorf("Error during binding queue: %s", err)
			return nil, err
		}

	}

	if err := w.ch.Qos(
		cfg.Limit, // prefetch count
		0,         // prefetch size
		false,     // global
	); err != nil {
		w.log.Error(err)
		return nil, err
	}

	s.workers[cfg.Name] = w

	return w, nil

}

func (w *Worker) Start() error {

	var err error

	w.deliveries, err = w.ch.Consume(
		w.queue, // queue
		w.name,  // consumer tag
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		w.log.Error(err)
		return err
	}

	go w.consume(w.deliveries)

	return nil

}

func (w *Worker) consume(deliveries <-chan amqp.Delivery) {

	w.log.Debugf("Worker %s started consuming", w.name)

	for {
		select {

		case <-w.stopConsume:

			w.log.Debug("Consuming stopped")
			return

		case d := <-deliveries:

			if len(d.Body) == 0 {

				if err := d.Nack(false, false); err != nil {
					w.log.Errorf("Consuming stopped: %v", err)
					return
				}

				w.log.Error("Empty task received")
				continue
			}

			w.tasksInProgress.Add(1)

			go w.consumeOne(d)

		}
	}

}

func (w *Worker) reconnect(con *amqp.Connection) error {

	w.log.Debugf("Worker '%s' reconnecting", w.name)

	var err error

	w.ch, err = con.Channel()
	if err != nil {
		return err
	}

	if err := w.ch.Qos(
		w.limit, // prefetch count
		0,       // prefetch size
		false,   // global
	); err != nil {
		return err
	}

	w.stopConsume = make(chan bool, 1)

	if err := w.Start(); err != nil {
		return err
	}

	return nil

}

func (w *Worker) Close() {

	w.log.Debug("Worker closing started")

	w.ch.Close()

	w.tasksInProgress.Wait()

	w.log.Debug("Worker is closed")

}

func (w *Worker) consumeOne(d amqp.Delivery) {
	defer w.tasksInProgress.Done()

	var err error

	var task Task
	if err := json.Unmarshal(d.Body, &task); err != nil {

		d.Nack(false, false)

		w.log.Error(errors.New("Can't unmarshal received task"))
		return
	}

	w.log.Debugf("Handling task %s", task.UUID)

	taskConfig, ok := w.tasks[task.Name]
	if !ok {
		d.Nack(false, true)
		return
	}

	reflectedTaskFunction := reflect.ValueOf(taskConfig.Function)

	reflectedTaskArgs, err := reflectArgs(task.Args)
	if err != nil {
		w.log.Errorf("Can't reflect task (%s) arguments: %v", task.UUID, err)
		return
	}

	tryCall(reflectedTaskFunction, reflectedTaskArgs, taskConfig.TimeoutSeconds)

	d.Ack(false)

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
