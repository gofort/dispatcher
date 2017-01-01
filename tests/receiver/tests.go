package main

import (
	"github.com/gofort/dispatcher"
	"github.com/gofort/dispatcher/tests/tasks"
	"github.com/sirupsen/logrus"
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
	t["compound"] = tasks.CompoundTaskConfig()

	w, err := server.NewWorker(
		&dispatcher.WorkerConfig{
			Limit:    10,
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

	select {}

}
