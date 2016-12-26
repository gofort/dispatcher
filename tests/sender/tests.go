package main

import (
	"github.com/gofort/dispatcher"
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

	task := dispatcher.Task{
		Name: "compound",
		Args: []dispatcher.TaskArgument{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	logrus.Info("Started")

	// 500 messages for 13 seconds if new channel for every delivery
	// 500 messages for 12 seconds if common channel for all publishers
	// 500 messages for 1 second if common channel for all publishers and every publish in goroutine
	// 500 messages for 1 second if new channel for every delivery and every publish in goroutine

	for {
		time.Sleep(time.Second * 2)
		err := server.PublishDefault(&task)
		if err != nil {
			logrus.Error(err)
		} else {
			logrus.Info("Sended")
		}
	}

}
