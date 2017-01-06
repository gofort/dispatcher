package dispatcher

import (
	"errors"
	"github.com/streadway/amqp"
	"testing"
)

func Test_bootstrapExchanges1(t *testing.T) {

	con, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer con.Close()

	ch, _ := con.Channel()
	defer ch.Close()

	err := bootstrapExchanges(ch, []Exchange{
		{"test_exchange_1", []Queue{
			{"queue_1", []string{"key_1"}},
		},
		}})
	if err != nil {
		t.Error(err)
		return
	}

	ch.ExchangeDelete("test_exchange_1", false, false)

}

func Test_bootstrapExchanges2(t *testing.T) {

	con, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer con.Close()

	ch, _ := con.Channel()
	defer ch.Close()

	err := bootstrapExchanges(ch, []Exchange{
		{"", []Queue{
			{"queue_1", []string{"key_1"}},
		},
		}})
	if err == nil {
		t.Error(errors.New("No error returned, but exchange was empty"))
		return
	}

	ch.ExchangeDelete("test_exchange_1", false, false)

}

func Test_bootstrapExchanges3(t *testing.T) {

	con, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer con.Close()

	ch, _ := con.Channel()
	defer ch.Close()

	err := bootstrapExchanges(ch, []Exchange{
		{"test_exchange_1", []Queue{
			{"", []string{"key_1"}},
		},
		}})
	if err == nil {
		t.Error(errors.New("No error returned, but queue was empty"))
		return
	}

	ch.ExchangeDelete("test_exchange_1", false, false)

}

func Test_bootstrapExchanges4(t *testing.T) {

	con, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer con.Close()

	ch, _ := con.Channel()
	defer ch.Close()

	err := bootstrapExchanges(ch, []Exchange{
		{"test_exchange_1", []Queue{
			{"queue_1", []string{""}},
		},
		}})
	if err == nil {
		t.Error(errors.New("No error returned, but binding key was empty"))
		return
	}

	ch.ExchangeDelete("test_exchange_1", false, false)

}
