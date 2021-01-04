package rabbitmqvndelay

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const activeExchange = "active.vn.exchange"
const delayExchange = "delay.vn.exchange"

const suffixDelayQueue = "delay.vn.queue"

var lock = &sync.Mutex{}

type RabbitMQVnDelay struct {
	channel  *amqp.Channel
	mapQueue map[string]bool
}

//NewRabbitMQ create new instance for rabbitMQ, used to call functional for Publish and PublishWithDelay
func NewRabbitMQ(connection *amqp.Connection) (*RabbitMQVnDelay, error) {

	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	r := &RabbitMQVnDelay{
		channel,
		map[string]bool{},
	}

	err = r.declareExchange(activeExchange)
	if err != nil {
		return nil, err
	}

	err = r.declareExchange(delayExchange)
	if err != nil {
		return nil, err
	}

	return r, nil
}

//Publish used to send message to queue without delay
func (r *RabbitMQVnDelay) Publish(queueName string, message string) error {
	var (
		err         error
		qName       = queueName
		routingName = queueName
	)

	if _, value := r.mapQueue[qName]; !value {
		err = r.initQueue(qName)
		if err != nil {
			return err
		}
	}

	err = r.publishActiveMessage(message, routingName, activeExchange)
	if err != nil {
		return err
	}

	return nil
}

//PublishWithDelay used to send message to queue with given specific delay, will accept time.Duration parameters
func (r *RabbitMQVnDelay) PublishWithDelay(queueName string, message string, delay time.Duration) error {
	var (
		err         error
		qName       = queueName
		routingName = queueName + "." + suffixDelayQueue
	)

	if _, value := r.mapQueue[qName]; !value {
		err = r.initQueue(qName)
		if err != nil {
			return err
		}
	}

	err = r.publishDelayMessage(message, routingName, delayExchange, delay)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) initQueue(queueName string) error {
	var (
		err         error
		qName       = queueName
		routingName = queueName
	)
	lock.Lock()
	defer lock.Unlock()

	if _, value := r.mapQueue[qName]; value {
		return nil
	}

	err = r.declareActiveQueue(qName)
	if err != nil {
		return err
	}

	err = r.bindQueueWithExchange(qName, routingName, activeExchange)
	if err != nil {
		return err
	}

	var (
		qNameDelay       = queueName + "." + suffixDelayQueue
		routingNameDelay = queueName + "." + suffixDelayQueue
	)

	err = r.declareDelayedQueue(qNameDelay, queueName)
	if err != nil {
		return err
	}

	err = r.bindQueueWithExchange(qNameDelay, routingNameDelay, delayExchange)
	if err != nil {
		return err
	}

	r.mapQueue[queueName] = true

	return nil
}

func (r *RabbitMQVnDelay) publishActiveMessage(message string, routingName string, exchangeName string) error {
	err := r.channel.Publish(
		exchangeName,
		routingName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(message),
		})
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) publishDelayMessage(message string, routingName string, exchangeName string, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("wrong delay value")
	}

	delayTime := strconv.FormatInt(delay.Milliseconds(), 10)
	err := r.channel.Publish(
		exchangeName,
		routingName,
		false,
		false,
		amqp.Publishing{
			Expiration:  delayTime,
			ContentType: "application/json",
			Body:        []byte(message),
		})
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) declareExchange(exchangeName string) error {
	var err error

	if err = r.channel.ExchangeDeclare(
		exchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) declareActiveQueue(queueName string) error {
	var err error

	_, err = r.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) declareDelayedQueue(queueName string, routingName string) error {
	var (
		err error
	)

	argsDelay1 := amqp.Table{
		"x-dead-letter-exchange":    activeExchange,
		"x-dead-letter-routing-key": routingName,
	}

	_, err = r.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		argsDelay1,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVnDelay) bindQueueWithExchange(queueName string, routingName string, exchangeName string) error {
	var err error

	if err = r.channel.QueueBind(
		queueName,
		routingName,
		exchangeName,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}
