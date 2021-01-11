package rabbitmqvndelay_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"

	r "github.com/sepulsa/rabbitmq-vn-delay"
)

const queueName = "vn.unit-test"
const acceptableDelay int64 = 5

var connection *amqp.Connection

func TestMain(m *testing.M) {
	var err error
	url := fmt.Sprintf("amqp://guest:guest@localhost:5672/")
	connection, err = amqp.Dial(url)
	if err != nil {
		os.Exit(-1)
	}

	os.Exit(m.Run())
}

func TestRabbitMQWithDelay(t *testing.T) {
	publisher, err := r.NewRabbitMQVNDelay(connection)

	if err != nil {
		t.Error("Failed to connect with rabbitmq")
		t.FailNow()
	}

	ch, err := connection.Channel()
	if err != nil {
		t.Error("failed to create channel. Error: ", err)
		t.FailNow()
	}

	messages, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	var waitgroup sync.WaitGroup

	go func() {
		for d := range messages {
			receivedTime := time.Now().Unix()
			messageInString := string(d.Body)
			messageInNumber, err := strconv.ParseInt(messageInString, 10, 64)
			if err != nil {
				t.Error("Failed on read message. Error: ", err)
			}

			delta := receivedTime - messageInNumber

			if delta > acceptableDelay {
				t.Error("Message received not on proper delay. Expect at: ", messageInNumber, " but received at: ", receivedTime)
			}

			d.Ack(false)
			waitgroup.Done()
		}
	}()

	waitgroup.Add(1)
	t.Run("Expect result published with delay", func(t *testing.T) {
		duration := time.Millisecond * 100
		message := strconv.FormatInt(time.Now().Add(duration).Unix(), 10)

		err = publisher.PublishWithDelay(queueName, message, duration)
		if err != nil {
			t.Error("Expect error is not nil on publish. Error: ", err)
			waitgroup.Done()
		}

	})

	waitgroup.Add(1)
	t.Run("Expect result published without delay", func(t *testing.T) {
		message := strconv.FormatInt(time.Now().Unix(), 10)

		err = publisher.Publish(queueName, message)
		if err != nil {
			t.Error("Expect error is not nil on publish. Error: ", err)
			waitgroup.Done()
		}
	})

	waitgroup.Wait()
}
