package project_1

import (
	"crypto/tls"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
	"github.com/gofort/dispatcher/log"
)

type ServerConfig struct {
	AMQPConnectionString        string
	ReconnectionRetries         int // TODO If 0 - close all workers immediately (every worker should have chan)
	ReconnectionIntervalSeconds int64
	TLSConfig                   *tls.Config
	SecureConnection            bool
	DebugMode                   bool // for default logger only
	PublishSettings             struct {
		DefaultExchange   string
		DefaultRoutingKey string
	}
}

type Server struct {
	con             *AMQPConnection
	log             Log
	Workers         map[string]Worker // TODO When reconnected - reinit all workers
	publishSettings struct {
		defaultExchange   string
		defaultRoutingKey string
	}
}

type AMQPConnection struct {
	Connection *amqp.Connection
	Close      chan bool
	mtx        sync.Mutex
	Connected  bool
}

func NewServer(cfg *ServerConfig) *Server {

	srv := new(Server)

	srv.log = log.InitLogger(cfg.DebugMode)

	srv.con = new(AMQPConnection)

	notifyConnected := make(chan bool)
	startGlobalShutoff := make(chan bool)
	workersFinished := make(chan bool)

	go srv.con.initConnection(srv.log, cfg, notifyConnected, startGlobalShutoff, workersFinished)

	// TODO After this reconnection wil not work because there are no other receivers for this chan, except cap below
	<- notifyConnected

	// Cap for channels
	go func (){
		for {
			select {
			case <- notifyConnected:
				srv.log.Info("Notify connected chan")
			case <- startGlobalShutoff:
				srv.log.Info("Start global shutoff chan")
			}
		}
	}()

	srv.log.Info("Connected to AMQP")

	return srv

}

func (s *Server) SetLogger(logger Log) {
	s.log = logger
}

func (s *AMQPConnection) initConnection(log Log, cfg *ServerConfig, notifyConnected chan bool, startGlobalShutoff chan bool, workersFinished chan bool) {

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

		counter = 0

		notifyConnected <- true

		s.Connected = true

		notifyClose := make(chan *amqp.Error)
		con.NotifyClose(notifyClose)

		select {
		case <-notifyClose:

			// TODO Pause workers

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
