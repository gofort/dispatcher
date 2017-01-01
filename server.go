package dispatcher

import (
	"errors"
	"github.com/gofort/dispatcher/log"
	"sync"
)

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

	if cfg.Logger == nil {
		srv.log = log.InitLogger(cfg.DebugMode)
	} else {
		srv.log = cfg.Logger
	}

	notifyConnected := make(chan bool)
	startGlobalShutoff := make(chan bool)
	workersFinished := make(chan bool)

	go srv.con.initConnection(srv.log, cfg, notifyConnected, startGlobalShutoff, workersFinished)

	<-notifyConnected

	// Cap for channels
	go func() {
		for {
			select {
			case connected := <-notifyConnected:

				if connected {

					srv.log.Info("Notify connected chan")

					err := srv.Publisher.init(srv.con.Connection)
					if err != nil {
						srv.log.Error(err)
					}

					for _, v := range srv.workers {
						err = v.reconnect(srv.con.Connection)
						if err != nil {
							srv.log.Error(err)
						}
					}

				} else {

					srv.Publisher.active = false

				}

			case <-startGlobalShutoff:
				srv.log.Info("Start global shutoff chan")

				srv.Publisher.deactivate()

				var wg sync.WaitGroup

				wg.Add(len(srv.workers))

				for _, v := range srv.workers {

					go func(w *Worker) {
						w.Close()
						wg.Done()
					}(v)

				}

				wg.Wait()

				workersFinished <- true

			}
		}
	}()

	srv.log.Info("Connected to AMQP")

	err := srv.Publisher.init(srv.con.Connection)
	if err != nil {
		srv.log.Error(err)
		return nil
	}

	if err = bootstrapExchanges(srv.Publisher.ch, cfg.InitExchanges); err != nil {
		srv.log.Error(err)
		return nil
	}

	return srv

}

func (s *Server) GetWorkerByName(name string) (*Worker, error) {

	worker, ok := s.workers[name]
	if !ok {
		return nil, errors.New("Worker not found")
	}

	return worker, nil

}
