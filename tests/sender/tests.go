package main

import (
	"github.com/gofort/dispatcher"
	"github.com/sirupsen/logrus"
)

func main() {

	servercfg := dispatcher.ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@127.0.0.1:5672/",
		ReconnectionRetries:         5,
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
	}

	servercfg.DefaultPublishSettings.Exchange = "dispatcher"
	servercfg.DefaultPublishSettings.RoutingKey = "routing_key_1"

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

	err := server.PublishDefault(&task)
	if err != nil {
		logrus.Error(err)
	}

}
