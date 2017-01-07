package main

import (
	"github.com/gofort/dispatcher"
	"log"
)

func main() {

	cfg := dispatcher.ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@localhost:5672/",
		ReconnectionRetries:         25,
		ReconnectionIntervalSeconds: 5,
		DebugMode:                   true, // enables extended logging
		Exchange:                    "dispatcher_example",
		InitQueues: []dispatcher.Queue{ // creates queues and binding keys if they are not created already
			{
				Name:        "queue_1",
				BindingKeys: []string{"dispatcher_rk"},
			},
		},
		DefaultRoutingKey: "dispatcher_rk", // default routing key which is used for publishing messages
	}

	// This function creates new server (server consists of AMQP connection and publisher which sends tasks)
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

	// Here we sending task to a queue
	if err := server.Publish(&task); err != nil {
		log.Println(err.Error())
		return
	}

	// Close server (close AMQP connection and destroy publisher)
	server.Close()

}
