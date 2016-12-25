package project_1

import "testing"

func TestServer_NewWorker(t *testing.T) {

	// TODO Create server from server config

	cfg := ServerConfig{
		AMQPConnectionString:        "amqp://guest:guest@127.0.0.1:5672/",
		ReconnectionRetries:         5,
		ReconnectionIntervalSeconds: 5,
		TLSConfig: nil,
		SecureConnection: false,
		DebugMode: true,
	}
	cfg.PublishSettings.DefaultExchange = "dispatcher"
	cfg.PublishSettings.DefaultRoutingKey = "dispatcher_default"

	NewServer(&cfg)

}
