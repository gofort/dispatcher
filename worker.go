package project_1

import "github.com/streadway/amqp"

type WorkerConfig struct {
	Limit        int    // Parallel tasks limit
	Exchange     string // dispatcher
	ExchangeType string // direct
	DefaultQueue string // scalet_backup, scalet_management, scalet_create
	BindingKey   string // If no routing key in task - this will be used, it is like default routing key for all tasks
	Tasks        map[string]TaskConfig
}

type TaskConfig struct {
	Timeout  int64 // in seconds
	Function interface{}
}

type Worker struct {
	Channel *amqp.Channel
	Queue   *amqp.Queue
	// TODO Chan that worker was closed
}

func (s *Server) NewWorker(cfg *WorkerConfig) (*Worker, error) {

	worker := new(Worker)

	ch, err := s.con.Channel()
	if err != nil {
		s.log.Errorf("Error during creating channel: %s", err)
		return nil, err
	}

	if err = ch.ExchangeDeclare(
		cfg.Exchange,     // name of the exchange
		cfg.ExchangeType, // type
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	); err != nil {
		s.log.Errorf("Error during declaring exchange: %s", err)
		return nil, err
	}

	// Declare a queue
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

	worker.Queue = &queue
	worker.Channel = ch

	return worker, nil

}
