package dispatcher

import "crypto/tls"

type ServerConfig struct {
	AMQPConnectionString        string
	ReconnectionRetries         int // TODO If 0 - close all workers immediately (every worker should have chan)
	ReconnectionIntervalSeconds int64
	TLSConfig                   *tls.Config
	SecureConnection            bool
	DebugMode                   bool // for default logger only
	InitExchanges               []Exchange
	DefaultPublishSettings      PublishSettings
	Logger                      Log
}

type PublishSettings struct {
	Exchange   string
	RoutingKey string
}

type Exchange struct {
	Name   string
	Queues []Queue
}

type Queue struct {
	Name        string
	BindingKeys []string
}
