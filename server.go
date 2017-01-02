package dispatcher

import (
	"errors"
	"github.com/gofort/dispatcher/log"
	"sync"
)

type Server struct {
	con                *amqpConnection
	notifyConnected    chan bool
	startGlobalShutoff chan struct{}

	log     Log
	workers map[string]*Worker
	*Publisher
	// TODO Task UUID as first argument?
}

func NewServer(cfg *ServerConfig) *Server {

	srv := &Server{
		Publisher: &Publisher{
			defaultRoutingKey: cfg.DefaultPublishSettings.RoutingKey,
			defaultExchange:   cfg.DefaultPublishSettings.Exchange,
		},
		startGlobalShutoff: make(chan struct{}),
		log:                log.InitLogger(cfg.DebugMode),
		notifyConnected:    make(chan bool),
		workers:            make(map[string]*Worker),
		con: &amqpConnection{
			workersFinished:  make(chan struct{}),
			stopReconnecting: make(chan struct{}),
		},
	}
	srv.Publisher.log = srv.log

	if cfg.Logger == nil {
		srv.log = log.InitLogger(cfg.DebugMode)
	} else {
		srv.log = cfg.Logger
	}

	go srv.con.initConnection(srv.log, cfg, srv.notifyConnected, srv.startGlobalShutoff)

	<-srv.notifyConnected

	go func() {
		for {
			select {
			case connected := <-srv.notifyConnected:

				if connected {

					err := srv.Publisher.init(srv.con.con)
					if err != nil {
						srv.log.Error(err)
					}

					for _, v := range srv.workers {
						err = v.reconnect(srv.con.con)
						if err != nil {
							srv.log.Error(err)
						}
					}

				} else {

					srv.Publisher.deactivate()

				}

			case <-srv.startGlobalShutoff:
				srv.log.Debug("Starting global shutoff: close publisher, stop workers consuming, wait for all tasks to be finished")

				srv.Publisher.deactivate()

				wg := new(sync.WaitGroup)

				wg.Add(len(srv.workers))

				for _, v := range srv.workers {

					go func(w *Worker, wg *sync.WaitGroup) {
						defer wg.Done()
						w.Close()
					}(v, wg)

				}

				srv.log.Debug("Waiting for all worker to be done")
				wg.Wait()
				srv.log.Debug("All workers finished their tasks and were closed!")

				srv.con.workersFinished <- struct{}{}

			}
		}
	}()

	err := srv.Publisher.init(srv.con.con)
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

func (s *Server) Close() {
	s.con.stopReconnecting <- struct{}{}
	s.con.close(s.log, s.startGlobalShutoff)
}
