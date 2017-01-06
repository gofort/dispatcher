package dispatcher

import (
	"testing"
	"time"
)

const serverTestExchange = "dispatcher_test"
const serverTestQueue = "test_queue"
const serverTestRoutingKey1 = "test_rk_1"
const serverTestRoutingKey2 = "test_rk_2"

func TestNewServer(t *testing.T) {

	cfg := ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@localhost:5672/",
		ReconnectionRetries:         5,
		ReconnectionIntervalSeconds: 5,
		TLSConfig:                   nil,
		SecureConnection:            false,
		DebugMode:                   true,
		InitExchanges:               []Exchange{{serverTestExchange, []Queue{{serverTestQueue, []string{serverTestRoutingKey1, serverTestRoutingKey2}}}}},
	}

	server, _ := NewServer(&cfg)

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

	ch2, err := server.con.con.Channel()
	if err != nil {
		t.Error(err)
		return
	}
	defer ch2.Close()

	ch2.ExchangeDelete(serverTestExchange, false, false)

	ch2.QueueDelete(serverTestQueue, false, false, false)

	server.Close()
	t.Log("Server is closed")

}
