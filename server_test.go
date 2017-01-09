package dispatcher

import (
	"errors"
	"os"
	"testing"
	"time"
)

const serverTestExchange = "dispatcher_test"
const serverTestQueue = "test_queue"
const serverTestRoutingKey1 = "test_rk_1"
const serverTestRoutingKey2 = "test_rk_2"

func TestNewServer(t *testing.T) {

	cfg := ServerConfig{
		AMQPConnectionString:        os.Getenv("DISPATCHER_AMQP_CON"),
		ReconnectionRetries:         5,
		ReconnectionIntervalSeconds: 5,
		TLSConfig:                   nil,
		SecureConnection:            false,
		DebugMode:                   true,
		Exchange:                    serverTestExchange,
		DefaultRoutingKey:           "test_rk_1",
		InitQueues:                  []Queue{{serverTestQueue, []string{serverTestRoutingKey1, serverTestRoutingKey2}}},
	}

	server, _, err := NewServer(&cfg)
	if err != nil {
		t.Error(err)
		return
	}
	defer server.Close()

	for {
		if server.con.connected {

			ch, err := server.con.con.Channel()
			if err != nil {
				t.Error(err)
				return
			}
			defer ch.Close()

			_, err = ch.QueueInspect(serverTestQueue)
			if err != nil {
				t.Error(err)
				return
			}

			if err := publishMessage(ch, serverTestExchange, serverTestRoutingKey1, make(map[string]interface{}), []byte("test_msg_1")); err != nil {
				t.Error(err)
				return
			}

			if err := publishMessage(ch, serverTestExchange, serverTestRoutingKey2, make(map[string]interface{}), []byte("test_msg_2")); err != nil {
				t.Error(err)
				return
			}

			t.Log("Server is connected, all exchanges, queries and binding keys were created")
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	_, err = server.NewWorker(&WorkerConfig{
		Limit:       5,
		Queue:       serverTestQueue,
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_1",
	}, make(map[string]TaskConfig))
	if err != nil {
		t.Error(err)
		return
	}

	_, err = server.NewWorker(&WorkerConfig{
		Limit:       5,
		Queue:       serverTestQueue,
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_1",
	}, make(map[string]TaskConfig))
	if err == nil {
		t.Error(errors.New("Worker with the same name was successfully created - expected error"))
		return
	}

	_, err = server.GetWorkerByName("test_worker_1")
	if err != nil {
		t.Error(err)
		return
	}

	_, err = server.GetWorkerByName("test_worker_2")
	if err == nil {
		t.Error(errors.New("GetWorkerByName returned worker which doesn't exist"))
		return
	}

	ch2, err := server.con.con.Channel()
	if err != nil {
		t.Error(err)
		return
	}
	defer ch2.Close()

	ch2.ExchangeDelete(serverTestExchange, false, false)

	ch2.QueueDelete(serverTestQueue, false, false, false)

}

func Test_ServerReconnecting(t *testing.T) {

	cfg := ServerConfig{
		AMQPConnectionString:        os.Getenv("DISPATCHER_AMQP_CON"),
		ReconnectionRetries:         5,
		ReconnectionIntervalSeconds: 5,
		TLSConfig:                   nil,
		SecureConnection:            false,
		DebugMode:                   true,
		Exchange:                    serverTestExchange,
		DefaultRoutingKey:           "test_rk_1",
		InitQueues:                  []Queue{{serverTestQueue, []string{serverTestRoutingKey1, serverTestRoutingKey2}}},
	}

	server, _, err := NewServer(&cfg)
	if err != nil {
		t.Error(err)
		return
	}
	defer server.Close()

	_, err = server.NewWorker(&WorkerConfig{
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_1",
	}, make(map[string]TaskConfig))
	if err == nil {
		t.Error(errors.New("Connection should not be created yet - error expected"))
		return
	}

	for {
		if server.con.connected {

			ch, err := server.con.con.Channel()
			if err != nil {
				t.Error(err)
				return
			}
			defer ch.Close()

			t.Log("Server is connected, all exchanges, queries and binding keys were created")
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	_, err = server.NewWorker(&WorkerConfig{
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_1",
	}, make(map[string]TaskConfig))
	if err == nil {
		t.Error(errors.New("No queue was passed in worker config, error expected"))
		return
	}

	_, err = server.NewWorker(&WorkerConfig{
		Queue:       serverTestQueue,
		BindingKeys: []string{serverTestRoutingKey1},
	}, make(map[string]TaskConfig))
	if err == nil {
		t.Error(errors.New("No name was passed in worker config, error expected"))
		return
	}

	_, err = server.NewWorker(&WorkerConfig{
		Queue:       serverTestQueue,
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_1",
	}, make(map[string]TaskConfig))
	if err != nil {
		t.Error(err)
		return
	}

	w2, err := server.NewWorker(&WorkerConfig{
		Limit:       6,
		Queue:       serverTestQueue,
		BindingKeys: []string{serverTestRoutingKey1},
		Name:        "test_worker_2",
	}, make(map[string]TaskConfig))
	if err != nil {
		t.Error(err)
		return
	}

	if err := w2.Start(server); err != nil {
		t.Error(err)
		return
	}

	time.Sleep(time.Second * 2)

	server.con.con.Close()
	time.Sleep(time.Second * 4)

	for {
		if server.con.connected {

			ch, err := server.con.con.Channel()
			if err != nil {
				t.Error(err)
				return
			}
			defer ch.Close()

			t.Log("Server is connected seconds time")
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	w2.Close()

	ch2, err := server.con.con.Channel()
	if err != nil {
		t.Error(err)
		return
	}
	defer ch2.Close()

	ch2.ExchangeDelete(serverTestExchange, false, false)

	ch2.QueueDelete(serverTestQueue, false, false, false)

}
