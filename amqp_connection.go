package dispatcher

import (
	"crypto/tls"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

type amqpConnection struct {
	con              *amqp.Connection // connection itself
	workersFinished  chan struct{}    // When all workers finished all their tasks connection receives a struct
	stopReconnecting chan struct{}    // When we are stopping server, we tak to connection that we don't need to reconnect anymore
	connected        bool             // If connection is alive - true, else - false
}

func (s *amqpConnection) initConnection(log Log, cfg *ServerConfig, notifyConnected chan bool, startGlobalShutoff chan struct{}) {

	// TODO Think about refactor for counter := 0 ; ...
	counter := 0

	for {

		if counter == cfg.ReconnectionRetries+1 {
			startGlobalShutoff <- struct{}{}
			return
		}

		var err error

		log.Debug("Trying to connect to AMQP")

		s.con, err = connectToAMQP(cfg.AMQPConnectionString, cfg.SecureConnection, cfg.TLSConfig)
		if err != nil {
			log.Error(err)
			counter++
			time.Sleep(time.Duration(cfg.ReconnectionIntervalSeconds) * time.Second)
			continue
		}

		log.Info("AMQP connection established")

		counter = 0
		notifyConnected <- true
		s.connected = true

		notifyClose := make(chan *amqp.Error)
		s.con.NotifyClose(notifyClose)

		select {
		case <-notifyClose:

			log.Info("AMQP connection was closed")

			notifyConnected <- false
			s.connected = false

			// Loop will be continued after this disconnection

		case <-s.stopReconnecting:
			log.Debug("Stopping automatic AMQP reconnection")
			return
		}

	}

}

func (s *amqpConnection) close(log Log, startGlobalShutoff chan struct{}) {

	log.Debug("Starting AMQP connection closing")

	startGlobalShutoff <- struct{}{}

	<-s.workersFinished

	log.Debug("All workers and their tasks were finished -> closing AMQP connection")

	s.con.Close()
	s.connected = false

	return
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
