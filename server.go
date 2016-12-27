package dispatcher

import (
	"crypto/tls"
	"errors"
	"github.com/gofort/dispatcher/log"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
)

type ServerConfig struct {
	AMQPConnectionString        string
	ReconnectionRetries         int // TODO If 0 - close all workers immediately (every worker should have chan)
	ReconnectionIntervalSeconds int64
	TLSConfig                   *tls.Config
	SecureConnection            bool
	DebugMode                   bool // for default logger only
	InitExchanges               []Exchange
	DefaultPublishSettings      PublishSettings
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

type Server struct {
	con             *amqpConnection
	log             Log
	workers         map[string]*Worker // TODO When reconnected - reinit all workers
	publishSettings struct {
		defaultExchange   string
		defaultRoutingKey string
	}
	confirmationChan chan amqp.Confirmation
	publishChannel   *amqp.Channel
	// TODO Maybe server should have one channel for publishing instead of opening new
}

type amqpConnection struct {
	Connection *amqp.Connection
	Close      chan bool
	mtx        sync.Mutex
	Connected  bool
}

func NewServer(cfg *ServerConfig) *Server {

	srv := new(Server)

	srv.publishSettings.defaultExchange = cfg.DefaultPublishSettings.Exchange
	srv.publishSettings.defaultRoutingKey = cfg.DefaultPublishSettings.RoutingKey

	srv.log = log.InitLogger(cfg.DebugMode)

	srv.con = new(amqpConnection)

	notifyConnected := make(chan bool)
	startGlobalShutoff := make(chan bool)
	workersFinished := make(chan bool)

	go srv.con.initConnection(srv.log, cfg, notifyConnected, startGlobalShutoff, workersFinished)

	// TODO After this reconnection wil not work because there are no other receivers for this chan, except cap below
	<-notifyConnected

	// Cap for channels
	go func() {
		for {
			select {
			case <-notifyConnected:
				srv.log.Info("Notify connected chan")

				srv.initPublishChannel()

				for k, v := range srv.workers {
					v.connect <- true
				}

			case <-startGlobalShutoff:

				for k, v := range srv.workers {
					v.stopConsume <- true
				}

				srv.log.Info("Start global shutoff chan")
			}
		}
	}()

	srv.log.Info("Connected to AMQP")

	err := srv.initPublishChannel()
	if err != nil {
		// TODO Handle error
		return nil
	}

	for _, e := range cfg.InitExchanges {

		if e.Name == "" {
			continue
		}

		err = declareExchange(srv.publishChannel, e.Name)
		if err != nil {
			srv.log.Error(err)
			continue
		}

		for _, q := range e.Queues {

			if q.Name == "" {
				continue
			}

			err = declareQueue(srv.publishChannel, q.Name)
			if err != nil {
				srv.log.Error(err)
				continue
			}

			for _, bk := range q.BindingKeys {

				if bk == "" {
					continue
				}

				err = queueBind(srv.publishChannel, e.Name, q.Name, bk)
				if err != nil {
					srv.log.Error(err)
					continue
				}

			}

		}

	}

	srv.log.Info("Exchanges, queues and binding keys added")

	return srv

}

func (srv *Server) initPublishChannel() error {

	ch, err := srv.con.Connection.Channel()
	if err != nil {
		srv.log.Error(err)
		return err
	}

	srv.publishChannel = ch

	if err = srv.publishChannel.Confirm(false); err != nil {
		return err
	}

	srv.confirmationChan = srv.publishChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	return nil

}

func (s *Server) SetLogger(logger Log) {
	s.log = logger
}

func (s *Server) GetWorkerByName(name string) (*Worker, error) {

	worker, ok := s.workers[name]
	if !ok {
		return nil, errors.New("Worker not found")
	}

	return worker, nil

}

func (s *amqpConnection) initConnection(log Log, cfg *ServerConfig, notifyConnected chan bool, startGlobalShutoff chan bool, workersFinished chan bool) {

	counter := 0

	for {

		if counter == cfg.ReconnectionRetries+1 {
			startGlobalShutoff <- true
			return
		}

		con, err := connectToAMQP(cfg.AMQPConnectionString, cfg.SecureConnection, cfg.TLSConfig)
		if err != nil {
			log.Error(err)
			counter++
			time.Sleep(time.Duration(cfg.ReconnectionIntervalSeconds) * time.Second)
			continue
		}

		s.Connection = con

		counter = 0

		notifyConnected <- true

		s.Connected = true

		notifyClose := make(chan *amqp.Error)
		s.Connection.NotifyClose(notifyClose)

		select {
		case <-notifyClose:

			s.Connected = false

		case <-s.Close:

			// Start workers closing operation
			startGlobalShutoff <- true

			// When all workers are finished - close connection
			<-workersFinished

			if err := s.Connection.Close(); err != nil {
				log.Error(err)
			}

			s.Connected = false

			return

		}

	}

}

func connectToAMQP(url string, secure bool, tlscfg *tls.Config) (*amqp.Connection, error) {

	if secure || tlscfg != nil || strings.HasPrefix(url, "amqps://") {

		if tlscfg == nil {

			tlscfg = &tls.Config{
				InsecureSkipVerify: true,
			}

		}

		url = strings.Replace(url, "amqp://", "amqps://", 1)

		return amqp.DialTLS(url, tlscfg)

	}

	return amqp.Dial(url)

}
