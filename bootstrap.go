package dispatcher

import (
	"errors"
	"github.com/streadway/amqp"
)

func bootstrap(ch *amqp.Channel, exchange string, queues []Queue) error {

	var err error

	if exchange != "" {
		err = declareExchange(ch, exchange)
		if err != nil {
			return err
		}
	}

	for _, q := range queues {

		if q.Name == "" {
			return errors.New("Empty queue name passed")
		}

		err = declareQueue(ch, q.Name)
		if err != nil {
			return err
		}

		for _, bk := range q.BindingKeys {

			if bk == "" {
				return errors.New("Empty binding key passed")
			}

			if exchange == "" {
				return errors.New("Can't bind key to queue without exchange")
			}

			err = queueBind(ch, exchange, q.Name, bk)
			if err != nil {
				return err
			}

		}

	}

	return nil

}
