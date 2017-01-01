package dispatcher

import (
	"crypto/tls"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
)

type amqpConnection struct {
	Connection *amqp.Connection
	Close      chan bool
	mtx        sync.Mutex
	Connected  bool
}

func (s *amqpConnection) initConnection(log Log, cfg *ServerConfig, notifyConnected chan bool, startGlobalShutoff chan bool, workersFinished chan bool) {

	// TODO Think about refactor for counter := 0 ; ...
	counter := 0

	for {

		if counter == cfg.ReconnectionRetries+1 {
			startGlobalShutoff <- true
			return
		}

		var err error

		s.Connection, err = connectToAMQP(cfg.AMQPConnectionString, cfg.SecureConnection, cfg.TLSConfig)
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
		s.Connection.NotifyClose(notifyClose)

		select {
		case <-notifyClose:

			s.Connected = false

			// Loop will be continued after this disconnection

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
