package main

import (
	"github.com/gofort/dispatcher"
	"github.com/gofort/dispatcher/tests/tasks"
	"github.com/sirupsen/logrus"
	"time"
)

func main() {

	servercfg := dispatcher.ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@127.0.0.1:5672/",
		ReconnectionRetries:         25,
		ReconnectionIntervalSeconds: 5,
		TLSConfig:                   nil,
		SecureConnection:            false,
		//DebugMode:                   true,
		InitExchanges: []dispatcher.Exchange{
			dispatcher.Exchange{
				Name: "dispatcher",
				Queues: []dispatcher.Queue{
					dispatcher.Queue{
						Name:        "queue_1",
						BindingKeys: []string{"routing_key_1"},
					},
					dispatcher.Queue{
						Name:        "queue_2",
						BindingKeys: []string{"routing_key_2"},
					},
				},
			},
		},
		DefaultPublishSettings: dispatcher.PublishSettings{
			Exchange:   "dispatcher",
			RoutingKey: "routing_key_1",
		},
	}

	server := dispatcher.NewServer(&servercfg)

	t1 := make(map[string]dispatcher.TaskConfig)
	t1["test_1"] = tasks.Test1TaskConfig()
	t1["test_4"] = tasks.Test4TaskConfig()
	t1["test_3"] = tasks.Test3TaskConfig()

	w1, err := server.NewWorker(
		&dispatcher.WorkerConfig{
			Limit:    2,
			Exchange: "dispatcher",
			Queue:    "queue_1",
			Name:     "worker_1",
		},
		t1,
	)

	if err != nil {
		logrus.Error(err)
		return
	}

	t2 := make(map[string]dispatcher.TaskConfig)
	t2["test_5"] = tasks.Test5TaskConfig()
	t2["test_2"] = tasks.Test2TaskConfig()

	w2, err := server.NewWorker(
		&dispatcher.WorkerConfig{
			Limit:    2,
			Exchange: "dispatcher",
			Queue:    "queue_2",
			Name:     "worker_2",
		},
		t2,
	)

	if err != nil {
		logrus.Error(err)
		return
	}

	err = w1.Start(server)
	if err != nil {
		logrus.Error(err)
		return
	}

	err = w2.Start(server)
	if err != nil {
		logrus.Error(err)
		return
	}

	time.Sleep(time.Second * 17)
	server.Close()

}
