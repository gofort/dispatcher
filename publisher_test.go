package dispatcher

import (
	"encoding/json"
	"errors"
	"github.com/gofort/dispatcher/log"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"testing"
)

const publisherTestExchange = "dispatcher_test"
const publisherTestQueue = "test_queue"
const publisherTestRoutingKey = "test_rk_1"

func createPublisherTestEnv() (*amqp.Connection, *publisher, error) {

	p := &publisher{
		log: log.InitLogger(true),
	}

	con, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, nil, err
	}

	ch, err := con.Channel()
	if err != nil {
		return nil, nil, err
	}
	defer ch.Close()

	if err := declareExchange(ch, publisherTestExchange); err != nil {
		return nil, nil, err
	}

	if err := declareQueue(ch, publisherTestQueue); err != nil {
		return nil, nil, err
	}

	if err := queueBind(ch, publisherTestExchange, publisherTestQueue, publisherTestRoutingKey); err != nil {
		return nil, nil, err
	}

	return con, p, nil
}

func destroyPublisherTestEnv(con *amqp.Connection, p *publisher) {

	p.ch.ExchangeDelete(publisherTestExchange, false, false)

	p.ch.QueueDelete(publisherTestExchange, false, false, false)

	p.deactivate()

	con.Close()

}

func newPublisherTestTask() *Task {
	return &Task{
		UUID: uuid.NewV4().String(),
		Name: "test_task_1",
		Args: []TaskArgument{{"string", "test string"}, {"int", 1}},
	}
}

func TestPublisher_Publish(t *testing.T) {
	as := assert.New(t)

	con, p, err := createPublisherTestEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer destroyPublisherTestEnv(con, p)

	p.defaultExchange = publisherTestExchange
	p.defaultRoutingKey = publisherTestRoutingKey

	if err = p.init(con); err != nil {
		t.Error(err)
		return
	}

	task := newPublisherTestTask()

	if err = p.Publish(task); err != nil {
		t.Error(err)
		return
	}

	q, err := p.ch.QueueInspect(publisherTestQueue)
	if err != nil {
		t.Error(err)
		return
	}

	as.Equal(1, q.Messages, "Number of messages in queue is not equal to 1")

	deliveries, err := p.ch.Consume(publisherTestQueue, "test_consumer_1", true, false, false, false, nil)
	if err != nil {
		t.Error(err)
		return
	}

	msg := <-deliveries

	var receivedTask Task

	if err = json.Unmarshal(msg.Body, &receivedTask); err != nil {
		t.Error(err)
		return
	}

	if task.UUID == receivedTask.UUID && task.Name == receivedTask.Name && task.Args[0].Type == receivedTask.Args[0].Type && task.Args[0].Value == receivedTask.Args[0].Value {
		return
	}

	t.Error("Sended task and received task are not equal")

}

func TestPublisher_PublishCustom(t *testing.T) {
	as := assert.New(t)

	con, p, err := createPublisherTestEnv()
	if err != nil {
		t.Error(err)
		return
	}
	defer destroyPublisherTestEnv(con, p)

	if err = p.init(con); err != nil {
		t.Error(err)
		return
	}

	task := newPublisherTestTask()
	task.UUID = ""
	task.Name = ""

	if err = p.PublishCustom(task, publisherTestExchange, publisherTestRoutingKey); err == nil {
		t.Error(errors.New("Task name was empty, error expected"))
		return
	}

	task.Name = "test_task_1"
	if err = p.PublishCustom(task, publisherTestExchange, publisherTestRoutingKey); err != nil {
		t.Error(err)
		return
	}

	q, err := p.ch.QueueInspect(publisherTestQueue)
	if err != nil {
		t.Error(err)
		return
	}

	as.Equal(1, q.Messages, "Number of messages in queue is not equal to 1")

	deliveries, err := p.ch.Consume(publisherTestQueue, "test_consumer_1", true, false, false, false, nil)
	if err != nil {
		t.Error(err)
		return
	}

	msg := <-deliveries

	var receivedTask Task

	if err = json.Unmarshal(msg.Body, &receivedTask); err != nil {
		t.Error(err)
		return
	}

	if task.UUID == receivedTask.UUID && task.Name == receivedTask.Name && task.Args[0].Type == receivedTask.Args[0].Type && task.Args[0].Value == receivedTask.Args[0].Value {
		return
	}

	t.Error("Sended task and received task are not equal")

}
