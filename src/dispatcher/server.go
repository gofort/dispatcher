package project_1

import (
	"src/github.com/streadway/amqp"
)

type Server struct {
	Connection *amqp.Connection
	Log        Log
	Workers    map[string]Worker // TODO When connection lost - reconnect all workers
}

type Worker struct {
	Channel *amqp.Channel
	Queue *amqp.Queue
}

func (s *Server) NewWorker(cfg *WorkerConfig) (*Worker, error) {

	worker := new(Worker)

	ch, err := s.Connection.Channel()
	if err != nil {
		s.Log.Errorf("Error during creating channel: %s", err)
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
		s.Log.Errorf("Error during declaring exchange: %s", err)
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
		s.Log.Errorf("Error during declaring queue: %s", err)
		return nil, err
	}

	if err := ch.QueueBind(
		queue.Name,     // name of the queue
		cfg.BindingKey, // binding key
		cfg.Exchange,   // source exchange
		false,          // noWait
		amqp.Table(map[string]interface{}{}), // TODO arguments, not implemented
	); err != nil {
		s.Log.Errorf("Error during binding queue: %s", err)
		return nil, err
	}

	worker.Queue = &queue
	worker.Channel = ch

	return worker, nil

}
