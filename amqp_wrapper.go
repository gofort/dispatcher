package dispatcher

import "github.com/streadway/amqp"

func declareExchange(ch *amqp.Channel, exchangeName string) error {
	return ch.ExchangeDeclare(
		exchangeName, // name of the exchange
		"direct",     // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
}

func declareQueue(ch *amqp.Channel, queueName string) error {

	_, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	return err
}

func queueBind(ch *amqp.Channel, exchange, queue, bindingKey string) error {
	return ch.QueueBind(
		queue,      // name of the queue
		bindingKey, // binding key
		exchange,   // source exchange
		false,      // noWait
		amqp.Table(map[string]interface{}{}),
	)
}

func publishMessage(ch *amqp.Channel, exchange, routingKey string, headers map[string]interface{}, msg []byte) error {
	return ch.Publish(exchange, routingKey, false, false, amqp.Publishing{
		Headers:      amqp.Table(headers),
		ContentType:  "application/json",
		Body:         msg,
		DeliveryMode: amqp.Persistent,
	})
}
