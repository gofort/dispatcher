// Package dispatcher is an asynchronous task queue/job queue based on distributed message passing.
package dispatcher

import "crypto/tls"

// ServerConfig is a configuration which needs for server creation.
//
// AMQPConnectionString example: amqp://guest:guest@localhost:5672/
//
// ReconnectionRetries - number of reconnection retries, when all retries exceed, server will be closed.
//
// ReconnectionIntervalSeconds - interval in seconds between every retry.
//
// SecureConnection - if true, uses TLSConfig with param InsecureSkipVerify.
//
// DebugMode - if true, enables debug level in logger (by default dispatcher uses logrus and this option enables debug level in it, if you use your own logger, omit this option).
//
// InitQueues - pass queues and binding keys to this field and server will create all of them if they don't exists.
//
// DefaultRoutingKey - default routing key for publishing messages.
//
// Logger - custom logger if you don't want to use dispatcher's default logrus.
type ServerConfig struct {
	AMQPConnectionString        string
	ReconnectionRetriesForever  bool
	ReconnectionRetries         int
	ReconnectionIntervalSeconds int64
	TLSConfig                   *tls.Config
	SecureConnection            bool
	DebugMode                   bool // for default logger only
	InitQueues                  []Queue
	Exchange                    string // required
	DefaultRoutingKey           string // required
	Logger                      Log
}

// Queue for creating during server creation.
// Has name and binding keys in it.
type Queue struct {
	Name        string
	BindingKeys []string
}
