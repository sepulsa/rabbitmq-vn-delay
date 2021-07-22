package rabbitmqvndelay_test

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	vndelay "github.com/sepulsa/rabbitmq-vn-delay"
)

const (
	queueName             = "vn.unit-test"
	acceptableDelay int64 = 150
)

var rabbitMQ *vndelay.RabbitMQVNDelay

func TestMain(m *testing.M) {
	var err error
	rabbitMQ, err = vndelay.NewRabbitMQVNDelay("amqp://guest:guest@localhost:5672/")

	if err != nil {
		log.Println("Error:", err)
		os.Exit(-1)
	}

	os.Exit(m.Run())
}

func TestPublish(t *testing.T) {
	var waitgroup sync.WaitGroup

	handler := func(data string, ack vndelay.DoneFn) {
		receivedTime := time.Now().Unix()
		messageInNumber, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			t.Error("Failed on read message. Error: ", err)
		}

		delta := receivedTime - messageInNumber

		if delta > acceptableDelay {
			t.Error("Message received not on proper delay. Expect at: ", messageInNumber, " but received at: ", receivedTime)
		}

		ack(true)
		waitgroup.Done()
	}

	rabbitMQ.Subscribe(queueName, 1, handler)
	rabbitMQ.Start()

	waitgroup.Add(1)
	t.Run("Expect result published with delay", func(t *testing.T) {
		duration := time.Millisecond * 100
		message := strconv.FormatInt(time.Now().Add(duration).Unix(), 10)

		err := rabbitMQ.PublishWithDelay(queueName, message, duration)
		if err != nil {
			t.Error("Expect error is not nil on publish. Error: ", err)
			waitgroup.Done()
		}
	})

	waitgroup.Add(1)
	t.Run("Expect result published without delay", func(t *testing.T) {
		message := strconv.FormatInt(time.Now().Unix(), 10)

		err := rabbitMQ.Publish(queueName, message)
		if err != nil {
			t.Error("Expect error is not nil on publish. Error: ", err)
			waitgroup.Done()
		}
	})

	waitgroup.Wait()
}

func TestSubscribe(t *testing.T) {
	const count = 100
	const noOfWorker = 2
	var waitgroup sync.WaitGroup

	go func() {
		time.Sleep(1 * time.Second)
		log.Println("Start publish queue")

		for i := 1; i <= count; i++ {
			data := fmt.Sprintf("%02d", i)

			log.Println("publish data:", data)
			err := rabbitMQ.Publish(queueName, data)

			if err != nil {
				t.Error(err)
			}

			time.Sleep(time.Millisecond * 30)
		}
	}()

	handler := func(data string, done vndelay.DoneFn) {
		log.Printf("execute task with data %s\n", data)
		time.Sleep(time.Millisecond * 100)
		done(true)
		waitgroup.Done()
	}

	rabbitMQ.Subscribe(queueName, noOfWorker, handler)

	startTime := time.Now()
	waitgroup.Add(count)

	rabbitMQ.Start()

	waitgroup.Wait()
	log.Printf("time consumption: %.2f\n", time.Now().Sub(startTime).Seconds())
}
