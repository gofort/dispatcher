package main

import (
	"github.com/gofort/dispatcher"
	"log"
)

func main() {

	// Here we declares what exchanges, queues and binding keys dispatcher should create during first start
	// You don't need this if they already created
	cfg := dispatcher.ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@localhost:5672/",
		ReconnectionRetries:         25,
		ReconnectionIntervalSeconds: 5,
		DebugMode:                   true,
		Exchange:                    "dispatcher_example",
		InitQueues: []dispatcher.Queue{
			{
				Name:        "queue_1",
				BindingKeys: []string{"dispatcher_rk"},
			},
		},
		DefaultRoutingKey: "dispatcher_rk",
	}

	server, _, err := dispatcher.NewServer(&cfg)
	if err != nil {
		log.Println(err.Error())
		return
	}

	task := dispatcher.Task{
		Name: "task_1",
		Args: []dispatcher.TaskArgument{
			{
				Type:  "string",
				Value: "simple string",
			},
			{
				Type:  "int",
				Value: 1,
			},
		},
	}

	if err := server.Publish(&task); err != nil {
		log.Println(err.Error())
		return
	}

	server.Close()

}
