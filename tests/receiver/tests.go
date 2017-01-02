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
		DebugMode:                   true,
		InitExchanges: []dispatcher.Exchange{
			dispatcher.Exchange{
				Name: "dispatcher",
				Queues: []dispatcher.Queue{
					dispatcher.Queue{
						Name:        "queue_1",
						BindingKeys: []string{"routing_key_1"},
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

	t := make(map[string]dispatcher.TaskConfig)
	t["test_1"] = tasks.Test1TaskConfig()
	t["test_2"] = tasks.Test2TaskConfig()
	t["test_3"] = tasks.Test3TaskConfig()

	w, err := server.NewWorker(
		&dispatcher.WorkerConfig{
			Limit:    3,
			Exchange: "dispatcher",
			Queue:    "queue_1",
			Name:     "worker_1",
		},
		t,
	)

	if err != nil {
		logrus.Error(err)
		return
	}

	if err = w.Start(); err != nil {
		logrus.Error(err)
		return
	}

	time.Sleep(time.Second * 7)
	select {}

}
