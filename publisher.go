package dispatcher

import (
	"encoding/json"
	"errors"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

type publisher struct {
	log Log

	ch               *amqp.Channel
	confirmationChan chan amqp.Confirmation
	active           bool

	defaultExchange   string
	defaultRoutingKey string
}

func (s *publisher) init(con *amqp.Connection) error {

	s.log.Debug("Publisher initialization")

	ch, err := con.Channel()
	if err != nil {
		return err
	}

	s.ch = ch

	if err = s.ch.Confirm(false); err != nil {
		return err
	}

	s.confirmationChan = s.ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	s.active = true

	s.log.Debug("Publisher is ready")

	return nil
}

func (s *publisher) deactivate() {

	s.log.Debug("Deactivating publisher")

	s.active = false
	s.ch.Close()

	s.log.Debug("Publisher is deactivated")

}

// This method is used for publishing tasks to certain exchanges and with certain routing keys.
// If passed exchange is empty - task exchange will be taken, if it is empty too - default exchange will be used.
// Behaviour with routing key is the same as with exchange.
func (s *publisher) PublishCustom(task *Task, exchange, routingKey string) error {

	if exchange == "" {

		if task.Exchange == "" {

			if s.defaultExchange == "" {
				return errors.New("No exchange passed")
			}

			task.Exchange = s.defaultExchange

		}

	} else {
		task.Exchange = exchange
	}

	if routingKey == "" {

		if task.RoutingKey == "" {

			if s.defaultRoutingKey == "" {
				return errors.New("No routing key passed")
			}

			task.RoutingKey = s.defaultRoutingKey

		}

	} else {
		task.RoutingKey = routingKey
	}

	return s.publishTask(task)

}

// This method is used for publishing tasks with default values.
// If task exchange is empty - default exchange will be taken.
// Behaviour with routing key is the same as with exchange.
func (s *publisher) Publish(task *Task) error {

	if task.Exchange == "" {
		if s.defaultExchange == "" {
			return errors.New("No exchange passed")
		}

		task.Exchange = s.defaultExchange

	}

	if task.RoutingKey == "" {
		if s.defaultRoutingKey != "" {
			return errors.New("No routing key passed")
		}

		task.RoutingKey = s.defaultRoutingKey

	}

	return s.publishTask(task)

}

func (s *publisher) publishTask(task *Task) error {

	if task.UUID == "" {
		task.UUID = uuid.NewV4().String()
	}

	if task.Name == "" {
		return errors.New("Task name was not passed")
	}

	msg, err := json.Marshal(task)
	if err != nil {
		return err
	}

	if !s.active {
		return errors.New("Service is disconnected")
	}

	err = s.ch.Publish(task.Exchange, task.RoutingKey, false, false, amqp.Publishing{
		Headers:      amqp.Table(task.Headers),
		ContentType:  "application/json",
		Body:         msg,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return err
	}

	confirmed := <-s.confirmationChan

	if confirmed.Ack {
		return nil
	}

	return errors.New("Failed to deliver message")
}
