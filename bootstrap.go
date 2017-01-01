package dispatcher

import (
	"errors"
	"github.com/streadway/amqp"
)

func bootstrapExchanges(ch *amqp.Channel, exchanges []Exchange) error {

	var err error

	for _, e := range exchanges {

		if e.Name == "" {
			return errors.New("Empty exchange name passed")
		}

		err = declareExchange(ch, e.Name)
		if err != nil {
			return err
		}

		for _, q := range e.Queues {

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

				err = queueBind(ch, e.Name, q.Name, bk)
				if err != nil {
					return err
				}

			}

		}

	}

}
