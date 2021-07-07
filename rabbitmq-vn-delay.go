package rabbitmqvndelay

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	activeExchange   = "active.vn.exchange"
	delayExchange    = "delay.vn.exchange"
	suffixDelayQueue = "delay.vn.queue"

	sleepDelay = time.Second * 2
	logTag     = "[vndelay]"
)

var lock = &sync.Mutex{}
var delayLock = &sync.Mutex{}

//RabbitMQVNDelay rabbitmq publish with delay supported
type RabbitMQVNDelay struct {
	connection    *amqp.Connection
	channel       *amqp.Channel
	mapQueue      map[string]bool
	delayMapQueue map[string]bool
	isClose       bool
}

//NewRabbitMQVNDelay create new instance for RabbitMQVNDelay, used to call functional for Publish and PublishWithDelay
func NewRabbitMQVNDelay(connection *amqp.Connection) (*RabbitMQVNDelay, error) {
	channel, err := connection.Channel()

	if err != nil {
		return nil, err
	}

	r := &RabbitMQVNDelay{
		connection,
		channel,
		map[string]bool{},
		map[string]bool{},
		false,
	}

	r.closeChannelHandler()

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

func (r *RabbitMQVNDelay) closeChannelHandler() {
	go func() {
		for {
			if r.isClose {
				log.Println(logTag, "channel was closed by request")
				break
			}

			reason, ok := <-r.channel.NotifyClose(make(chan *amqp.Error))

			if !ok {
				log.Println(logTag, "channel was normally closed")
				return
			}

			log.Println(logTag, "channel was accidentally closed with a reason:", reason)

			for {
				if r.isClose {
					log.Println(logTag, "channel was closed by request")
					break
				}

				if r.connection == nil {
					//delay for re-create
					time.Sleep(sleepDelay)
					continue
				}

				channel, err := r.connection.Channel()

				if err == nil {
					r.channel = channel
					log.Println(logTag, "success to re-create channel")
					break
				}

				//delay for re-create
				time.Sleep(sleepDelay)
			}
		}
	}()
}

//Publish used to send message to queue without delay
func (r *RabbitMQVNDelay) Publish(queueName string, message string) error {
	if r.isClose {
		return errors.New("Publish was stopped")
	}

	var (
		err        error
		routingKey = queueName
	)

	if _, value := r.mapQueue[queueName]; !value {
		err = r.initQueue(queueName)
		if err != nil {
			return err
		}
	}

	err = r.publishActiveMessage(message, routingKey, activeExchange)
	if err != nil {
		return err
	}

	return nil
}

//PublishWithDelay used to send message to queue with given specific delay, will accept time.Duration parameters
func (r *RabbitMQVNDelay) PublishWithDelay(queueName string, message string, delay time.Duration) error {
	if r.isClose {
		return errors.New("Publish was stopped")
	}

	var (
		err        error
		routingKey = queueName + "." + suffixDelayQueue
	)

	if _, value := r.delayMapQueue[queueName]; !value {
		err = r.initDelayQueue(queueName)
		if err != nil {
			return err
		}
	}

	err = r.publishDelayMessage(message, routingKey, delayExchange, delay)
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVNDelay) Close() {
	if r.channel != nil {
		r.channel.Close()
	}

	r.isClose = true
}

func (r *RabbitMQVNDelay) initQueue(queueName string) error {
	var (
		err        error
		routingKey = queueName
	)
	lock.Lock()
	defer lock.Unlock()

	if _, value := r.mapQueue[queueName]; value {
		return nil
	}

	err = r.declareActiveQueue(queueName)
	if err != nil {
		return err
	}

	err = r.bindQueueWithExchange(queueName, routingKey, activeExchange)
	if err != nil {
		return err
	}

	r.mapQueue[queueName] = true

	return nil
}

func (r *RabbitMQVNDelay) initDelayQueue(queueName string) error {
	var (
		err             error
		delayQueueName  = queueName + "." + suffixDelayQueue
		delayroutingKey = queueName + "." + suffixDelayQueue
	)
	delayLock.Lock()
	defer delayLock.Unlock()

	if _, value := r.delayMapQueue[queueName]; value {
		return nil
	}

	r.initQueue(queueName)

	err = r.declareDelayedQueue(delayQueueName, queueName)
	if err != nil {
		return err
	}

	err = r.bindQueueWithExchange(delayQueueName, delayroutingKey, delayExchange)
	if err != nil {
		return err
	}

	r.delayMapQueue[queueName] = true

	return nil
}

func (r *RabbitMQVNDelay) publishActiveMessage(message string, routingKey string, exchangeName string) error {
	err := r.channel.Publish(
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(message),
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVNDelay) publishDelayMessage(message string, routingKey string, exchangeName string, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("wrong delay value")
	}

	delayTime := strconv.FormatInt(delay.Milliseconds(), 10)

	err := r.channel.Publish(
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Expiration:   delayTime,
			ContentType:  "text/plain",
			Body:         []byte(message),
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQVNDelay) declareExchange(exchangeName string) error {
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

func (r *RabbitMQVNDelay) declareActiveQueue(queueName string) error {
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

func (r *RabbitMQVNDelay) declareDelayedQueue(queueName string, routingKey string) error {
	var (
		err error
	)

	argsDelay1 := amqp.Table{
		"x-dead-letter-exchange":    activeExchange,
		"x-dead-letter-routing-key": routingKey,
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

func (r *RabbitMQVNDelay) bindQueueWithExchange(queueName string, routingKey string, exchangeName string) error {
	var err error

	if err = r.channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}
