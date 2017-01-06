package dispatcher

import (
	"encoding/json"
	"github.com/gofort/dispatcher/log"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"sync"
	"testing"
	"time"
)

const workerTestExchange = "dispatcher_test"
const workerTestQueue = "test_queue"
const workerTestRoutingKey = "test_rk_1"
const workerTestName = "test_worker_1"
const workerTestLimit = 3

func createWorkerTestEnv(test1, test2, test3 chan struct{}) (*amqp.Connection, *Worker, error) {

	w := &Worker{
		log:             log.InitLogger(true),
		name:            workerTestName,
		tasks:           newWorkerTestTaskConfigs(test1, test2, test3),
		limit:           workerTestLimit,
		queue:           workerTestQueue,
		tasksInProgress: new(sync.WaitGroup),
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

	if err := declareExchange(ch, workerTestExchange); err != nil {
		return nil, nil, err
	}

	if err := declareQueue(ch, workerTestQueue); err != nil {
		return nil, nil, err
	}

	if err := queueBind(ch, workerTestExchange, workerTestQueue, workerTestRoutingKey); err != nil {
		return nil, nil, err
	}

	return con, w, nil
}

func destroyWorkerTestEnv(con *amqp.Connection) {

	ch, _ := con.Channel()

	ch.ExchangeDelete(publisherTestExchange, false, false)

	ch.QueueDelete(publisherTestExchange, false, false, false)

	ch.Close()
	con.Close()

}

func newWorkerTestTaskConfigs(test1, test2, test3 chan struct{}) map[string]TaskConfig {

	t := make(map[string]TaskConfig)

	t["test_task_1"] = TaskConfig{
		TimeoutSeconds: 30,
		Function: func(taskuuid string, str string, someint int) {
			test1 <- struct{}{}
		},
		TaskUUIDAsFirstArg: true,
	}

	t["test_task_2"] = TaskConfig{
		TimeoutSeconds: 30,
		Function: func(
			tbool bool,
			tstring string,
			tint int, tint8 int8, tint16 int16, tint32 int32, tint64 int64,
			tuint uint, tuint8 uint8, tuint16 uint16, tuint32 uint32, tuint64 uint64,
			tfloat32 float32, tfloat64 float64) {
			test2 <- struct{}{}
		},
	}

	t["test_task_3"] = TaskConfig{
		TimeoutSeconds: 3,
		Function: func() (string, error) {
			time.Sleep(time.Second * 9)
			test3 <- struct{}{}
			return "", nil
		},
	}

	return t

}

func newWorkerTest1Task() (*Task, []byte) {

	t := Task{
		UUID:       uuid.NewV4().String(),
		Name:       "test_task_1",
		Args:       []TaskArgument{{"string", "test string"}, {"int", 1}},
		RoutingKey: workerTestRoutingKey,
	}

	msg, _ := json.Marshal(t)

	return &t, msg
}

func newWorkerTest2Task() (*Task, []byte) {

	t := Task{
		UUID: uuid.NewV4().String(),
		Name: "test_task_2",
		Args: []TaskArgument{
			{"bool", true},
			{"string", "test string"},
			{"int", 1}, {"int8", 8}, {"int16", 16}, {"int32", 32}, {"int64", 64},
			{"uint", 1}, {"uint8", 8}, {"uint16", 16}, {"uint32", 32}, {"uint64", 64},
			{"float32", 0.32}, {"float64", 0.64}},
		RoutingKey: workerTestRoutingKey,
	}

	msg, _ := json.Marshal(t)

	return &t, msg
}

func newWorkerTest3Task() (*Task, []byte) {

	t := Task{
		UUID:       uuid.NewV4().String(),
		Name:       "test_task_3",
		RoutingKey: workerTestRoutingKey,
	}

	msg, _ := json.Marshal(t)

	return &t, msg
}

func TestWorker(t *testing.T) {

	test1 := make(chan struct{})
	test2 := make(chan struct{})
	test3 := make(chan struct{})

	con, w, err := createWorkerTestEnv(test1, test2, test3)
	if err != nil {
		t.Error(err)
		return
	}
	defer destroyWorkerTestEnv(con)

	ch, err := con.Channel()
	if err != nil {
		t.Error(err)
		return
	}

	if err = w.init(con); err != nil {
		t.Error(err)
		return
	}

	task1, msg1 := newWorkerTest1Task()

	if err = publishMessage(ch, publisherTestExchange, task1.RoutingKey, task1.Headers, msg1); err != nil {
		t.Error(err)
		return
	}

	<-test1
	t.Log("Task 1 with TaskUUIDAsFirstArg parameter passed!")

	task2, msg2 := newWorkerTest2Task()

	if err = publishMessage(ch, publisherTestExchange, task2.RoutingKey, task2.Headers, msg2); err != nil {
		t.Error(err)
		return
	}

	<-test2
	t.Log("Task 2 with all types of arguments passed!")

	task3, msg3 := newWorkerTest3Task()

	if err = publishMessage(ch, publisherTestExchange, task3.RoutingKey, task3.Headers, msg3); err != nil {
		t.Error(err)
		return
	}

	timer := time.NewTimer(time.Second * 6)

	select {
	case <-test2:
		t.Error("Timeout of task 3 hadn't worked")
	case <-timer.C:
		t.Log("Task 3 without arguments and with timeout passed!")
	}

	w.Close()

}
