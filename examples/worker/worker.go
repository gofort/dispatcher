package main

import (
	"github.com/gofort/dispatcher"
	"log"
	"os"
	"os/signal"
	"syscall"
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

	// Basic worker configuration
	workercfg := dispatcher.WorkerConfig{
		Queue: "queue_1",
		Name:  "worker_1",
	}

	tasks := make(map[string]dispatcher.TaskConfig)

	// Task configuration where we pass function which will be executed by this worker when this task will be received
	tasks["task_1"] = dispatcher.TaskConfig{
		Function: func(str string, someint int) {
			log.Printf("Example function arguments: string - %s, int - %d\n", str, someint)
			log.Println("Example function completed!")
		},
	}

	// This function creates worker, but he won't start to consume messages here
	worker, err := server.NewWorker(&workercfg, tasks)
	if err != nil {
		log.Println(err.Error())
		return
	}

	// Here we start worker consuming
	if err := worker.Start(server); err != nil {
		log.Println(err.Error())
		return
	}

	sig := make(chan os.Signal, 1)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	// When you push CTRL+C close worker gracefully
	<-sig

	server.Close()

}
