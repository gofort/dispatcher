package dispatcher

import (
	"errors"
	"github.com/gofort/dispatcher/log"
	"sync"
)

// Server contains AMQP connection and creates publisher.
// Server is a parent of workers and publisher.
type Server struct {
	con                *amqpConnection
	notifyConnected    chan bool
	startGlobalShutoff chan struct{}

	log     Log
	workers map[string]*Worker
	*publisher
}

// NewServer creates new server from config and connects to AMQP.
func NewServer(cfg *ServerConfig) (*Server, chan struct{}) {

	srv := &Server{
		publisher: &publisher{
			defaultRoutingKey: cfg.DefaultPublishSettings.RoutingKey,
			defaultExchange:   cfg.DefaultPublishSettings.Exchange,
		},
		startGlobalShutoff: make(chan struct{}),
		notifyConnected:    make(chan bool),
		workers:            make(map[string]*Worker),
		con: &amqpConnection{
			workersFinished:  make(chan struct{}),
			stopReconnecting: make(chan struct{}),
		},
	}

	if cfg.Logger == nil {
		srv.log = log.InitLogger(cfg.DebugMode)
	} else {
		srv.log = cfg.Logger
	}

	srv.publisher.log = srv.log

	if cfg.DefaultPublishSettings.Exchange == "" || cfg.DefaultPublishSettings.RoutingKey == "" {
		srv.log.Info("You havn't passed default exchange or default routing key for publisher in config. " +
			"This means that you need to fill exchange and routing key for every task manually via PublishCustom method or " +
			"via exchange and routing key of task itself.")
	}

	connectionBroken := make(chan struct{})

	go srv.con.initConnection(srv.log, cfg, srv.notifyConnected, srv.startGlobalShutoff, connectionBroken)

	<-srv.notifyConnected

	go func() {
		for {
			select {
			case connected := <-srv.notifyConnected:

				if connected {

					err := srv.publisher.init(srv.con.con)
					if err != nil {
						srv.log.Error(err)
					}

					for _, v := range srv.workers {
						if v.working {
							err = v.Start(srv)
							if err != nil {
								srv.log.Error(err)
							}
						}
					}

				} else {

					srv.publisher.deactivate()

				}

			case <-srv.startGlobalShutoff:
				srv.log.Debug("Starting global shutoff: close publisher, stop workers consuming, wait for all tasks to be finished")

				srv.publisher.deactivate()

				wg := new(sync.WaitGroup)

				wg.Add(len(srv.workers))

				for _, v := range srv.workers {

					go func(w *Worker, wg *sync.WaitGroup) {
						defer wg.Done()
						if w.working {
							w.Close()
						}
					}(v, wg)

				}

				srv.log.Info("Waiting for all workers to be done")
				wg.Wait()
				srv.log.Info("All workers finished their tasks and were closed!")

				srv.con.workersFinished <- struct{}{}

			}
		}
	}()

	err := srv.publisher.init(srv.con.con)
	if err != nil {
		srv.log.Error(err)
		return nil, nil
	}

	if err = bootstrapExchanges(srv.publisher.ch, cfg.InitExchanges); err != nil {
		srv.log.Error(err)
		return nil, nil
	}

	return srv, connectionBroken

}

// GetWorkerByName returns a pointer to a Worker by its name.
func (s *Server) GetWorkerByName(name string) (*Worker, error) {

	worker, ok := s.workers[name]
	if !ok {
		return nil, errors.New("Worker not found")
	}

	return worker, nil

}

// Close is a complicated function which handles graceful quit of everything which dispatcher
// has (workers, publisher and connection).
// At first it stops reconnection process, then it closes publisher, after this it closes all
// workers and waits until all of them will finish their tasks and closes their channels.
// After all of this it closes AMQP connection.
func (s *Server) Close() {
	s.con.stopReconnecting <- struct{}{}
	s.con.close(s.log, s.startGlobalShutoff)
}
