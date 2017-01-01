package dispatcher

import "github.com/gofort/dispatcher/log"

type Server struct {
	con     *amqpConnection
	log     Log
	workers map[string]*Worker
	*Publisher
}

func NewServer(cfg *ServerConfig) *Server {

	srv := &Server{
		Publisher: &Publisher{
			defaultRoutingKey: cfg.DefaultPublishSettings.RoutingKey,
			defaultExchange:   cfg.DefaultPublishSettings.Exchange,
		},
		log:     log.InitLogger(cfg.DebugMode),
		workers: make(map[string]*Worker),
		con:     new(amqpConnection),
	}

}
