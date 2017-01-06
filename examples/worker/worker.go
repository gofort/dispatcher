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

	workercfg := dispatcher.WorkerConfig{
		Queue: "queue_1",
		Name:  "worker_1",
	}

	tasks := make(map[string]dispatcher.TaskConfig)

	tasks["task_1"] = dispatcher.TaskConfig{
		Function: func(str string, someint int) {
			log.Printf("Example function arguments: string - %s, int - %d\n", str, someint)
			log.Println("Example function completed!")
		},
	}

	worker, err := server.NewWorker(&workercfg, tasks)
	if err != nil {
		log.Println(err.Error())
		return
	}

	if err := worker.Start(server); err != nil {
		log.Println(err.Error())
		return
	}

	sig := make(chan os.Signal, 1)

	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	server.Close()

}
