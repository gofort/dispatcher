package project_1

import (
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
)

func (s *Server) Publish(task *Task, exchange, routingKey string) error {

	if task.Exchange == "" {
		if exchange != "" {
			task.Exchange = exchange
		} else {
			if s.publishSettings.defaultExchange != "" {
				task.Exchange = s.publishSettings.defaultExchange
			} else {
				return errors.New("No exchange passed")
			}
		}
	}

	if task.RoutingKey == "" {
		if routingKey != "" {
			task.RoutingKey = routingKey
		} else {
			if s.publishSettings.defaultRoutingKey != "" {
				task.RoutingKey = s.publishSettings.defaultRoutingKey
			} else {
				return errors.New("No routing key passed")
			}
		}
	}

	return s.publishTask(task)

}

func (s *Server) PublishDefault(task *Task) error {

	if task.Exchange == "" {
		if s.publishSettings.defaultExchange != "" {
			task.Exchange = s.publishSettings.defaultExchange
		} else {
			return errors.New("No exchange passed")
		}
	}

	if task.RoutingKey == "" {
		if s.publishSettings.defaultRoutingKey != "" {
			task.RoutingKey = s.publishSettings.defaultRoutingKey
		} else {
			return errors.New("No routing key passed")
		}
	}

	return s.publishTask(task)

}

func (s *Server) publishTask(task *Task) error {
	msg, err := json.Marshal(task)
	if err != nil {
		return err
	}

	if !s.con.Connected {
		return errors.New("Service is disconnected")
	}

	ch, err := s.con.Connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.Publish(task.Exchange, task.RoutingKey, false, false, amqp.Publishing{
		Headers:      amqp.Table(task.Headers),
		ContentType:  "application/json",
		Body:         msg,
		DeliveryMode: amqp.Persistent,
	})
	if err != nil {
		return err
	}

	return nil
}
