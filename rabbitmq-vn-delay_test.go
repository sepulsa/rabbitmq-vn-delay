package rabbitmqvndelay_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/streadway/amqp"

	r "github.com/sepulsa/rabbitmq-vn-delay"
)

const queueName = "vn.unit-test"
const dataForPublish = "just use for unit test"

func TestRabbitMQWithDelay(t *testing.T) {
	publisher := setupTest(t)

	t.Run("Expect result published with delay correct", func(t *testing.T) {
		err := publisher.PublishWithDelay(queueName, dataForPublish, time.Second*1)
		if err != nil {
			t.Error("error on publish process", err)
		}
		time.Sleep(2 * time.Second)

		queueResult := getPublishedData()
		if queueResult == "" {
			t.Error("Failed to get published data")
		} else if queueResult != dataForPublish {
			t.Error("missmatch data for publish")
		}
	})

	t.Run("Expect result published with delay error because time less than zero", func(t *testing.T) {
		err := publisher.PublishWithDelay(queueName, dataForPublish, -1)
		if err == nil {
			t.Error("error on publish process", err)
		}
	})
}

func setupTest(t *testing.T) *r.RabbitMQVnDelay {
	url := fmt.Sprintf("amqp://guest:guest@localhost:5672/")
	connection, err := amqp.Dial(url)
	if err != nil {
		t.Error("error dial", err)
	}

	publisher, err := r.NewRabbitMQ(connection)
	if err != nil {
		t.Error("error new instance", err)
	}

	return publisher
}

func getPublishedData() string {
	var body string

	url := fmt.Sprintf("amqp://guest:guest@localhost:5672/")
	connection, err := amqp.Dial(url)
	if err != nil {
		return ""
	}

	ch, err := connection.Channel()
	if err != nil {
		return ""
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

	finish := make(chan bool)
	go func() {
		for d := range messages {
			body = string(d.Body)
			log.Printf("Received a message: %s", d.Body)
			log.Printf("Done")
			d.Ack(false)
			finish <- true
		}
	}()

	<-finish
	return body
}
