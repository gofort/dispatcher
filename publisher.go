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

// Publish method is used for publishing tasks.
func (s *publisher) Publish(task *Task) error {

	if task.RoutingKey == "" {
		task.RoutingKey = s.defaultRoutingKey
	}

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

	if err := publishMessage(s.ch, s.defaultExchange, task.RoutingKey, task.Headers, msg); err != nil {
		return err
	}

	confirmed := <-s.confirmationChan

	if confirmed.Ack {
		return nil
	}

	return errors.New("Failed to deliver message")

}
