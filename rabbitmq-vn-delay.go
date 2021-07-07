package rabbitmqvndelay

import (
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

type AckFn func() error
type HandlerFn func(data string, ack AckFn)

type queueSubscription struct {
	queueName  string
	noOfWorker uint16
	handler    HandlerFn
	channel    *amqp.Channel
}

//RabbitMQVNDelay rabbitmq publish with delay supported
type RabbitMQVNDelay struct {
	connection     *amqp.Connection
	publishChannel *amqp.Channel
	mapQueue       map[string]bool
	delayMapQueue  map[string]bool
	queueSubs      []queueSubscription
}

//NewRabbitMQVNDelay create new instance for RabbitMQVNDelay, used to call functional for Publish and PublishWithDelay
func NewRabbitMQVNDelay(url string) (*RabbitMQVNDelay, error) {
	r := &RabbitMQVNDelay{
		nil,
		nil,
		map[string]bool{},
		map[string]bool{},
		make([]queueSubscription, 0),
	}

	err := r.dial(url)
	if err != nil {
		log.Println("failed connect to rabbitMQ server. Error: ", err)
		return nil, err
	}

	r.initPublishChannel()

	err = r.declareExchange(activeExchange)
	if err != nil {
		r.publishChannel.Close()
		r.connection.Close()
		return nil, err
	}

	err = r.declareExchange(delayExchange)
	if err != nil {
		r.publishChannel.Close()
		r.connection.Close()
		return nil, err
	}

	return r, nil
}

//Publish used to send message to queue without delay
func (r *RabbitMQVNDelay) Publish(queueName string, message string) error {
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

//Subscribe rabbitmq subscriber
func (r *RabbitMQVNDelay) Subscribe(queueName string, noOfWorker uint16, handler HandlerFn) {
	queueSub := queueSubscription{
		queueName,
		noOfWorker,
		handler,
		nil,
	}

	r.queueSubs = append(r.queueSubs, queueSub)
}

func (r *RabbitMQVNDelay) Start() error {
	queueDeclareChannel, err := r.connection.Channel()
	defer queueDeclareChannel.Close()

	if err != nil {
		log.Println(logTag, "failed to initialize subscribe channel. Error:", err)
		return err
	}

	for idx, queueSub := range r.queueSubs {
		_, err = queueDeclareChannel.QueueDeclare(
			queueSub.queueName,
			true,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			log.Printf("%s failed declare queue for %s. Error: %v\n", logTag, queueSub.queueName, err)

			//close all channel created before (rollback)
			for i := 0; i < idx; i++ {
				r.queueSubs[i].channel.Close()
			}

			return err
		}

		r.listen(idx)
	}

	return nil
}

func (r *RabbitMQVNDelay) listen(index int) error {
	var err error
	queueName := r.queueSubs[index].queueName

	for {
		if r.connection == nil {
			//delay for re-create
			time.Sleep(sleepDelay)
			continue
		}

		r.queueSubs[index].channel, err = r.connection.Channel()

		if err == nil {
			break
		}

		log.Printf("%s failed to init channel for %s. Error: %v\n", logTag, queueName, err)

		//delay for re-create
		time.Sleep(sleepDelay)
	}

	go func() {
		reason, ok := <-r.queueSubs[index].channel.NotifyClose(make(chan *amqp.Error))

		if !ok {
			log.Println(logTag, "channel was normally closed")
			return
		}

		log.Printf("%s channel was closed for %s. Reason: %v\n", logTag, queueName, reason)

		//delay for re-create
		time.Sleep(sleepDelay)

		r.listen(index)
	}()

	r.subscribe(index)

	return nil
}

func (r *RabbitMQVNDelay) subscribe(index int) {
	var messages <-chan amqp.Delivery
	var noOfWorker int = int(r.queueSubs[index].noOfWorker)

	queueName := r.queueSubs[index].queueName

	for {
		err := r.queueSubs[index].channel.Qos(noOfWorker, 0, false)

		if err != nil {
			log.Printf("%s failed to set channel QOS for %s. Error: %v\n", logTag, queueName, err)

			//delay for re-create
			time.Sleep(sleepDelay)
			continue
		}

		messages, err = r.queueSubs[index].channel.Consume(
			queueName,
			"",
			false,
			false,
			false,
			true,
			nil,
		)

		if err != nil {
			log.Println("failed declare rabbitMQ consumer", err)

			//delay for re-create
			time.Sleep(sleepDelay)
			continue
		}

		//break if everything is successfull
		break
	}

	for i := 0; i < noOfWorker; i++ {
		go func() {
			for {
				for data := range messages {
					// message := string(data.Body)
					// log.Printf("%s [%s] process message: %s\n", logTag, queueName, message)

					ackFn := func() error {
						// log.Printf("%s [%s] ack message: %s\n", logTag, queueName, message)
						return data.Ack(false)
					}

					r.queueSubs[index].handler(string(data.Body), ackFn)
				}
			}
		}()
	}
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
	err := r.publishChannel.Publish(
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

	err := r.publishChannel.Publish(
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

	if err = r.publishChannel.ExchangeDeclare(
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

	_, err = r.publishChannel.QueueDeclare(
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

	_, err = r.publishChannel.QueueDeclare(
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

	if err = r.publishChannel.QueueBind(
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

func (r *RabbitMQVNDelay) initPublishChannel() {
	for {
		if r.connection == nil {
			//delay for re-create
			time.Sleep(sleepDelay)
			continue
		}

		channel, err := r.connection.Channel()

		if err == nil {
			r.publishChannel = channel
			break
		}

		log.Println(logTag, "failed to init publish channel. Error:", err)

		//delay for re-create
		time.Sleep(sleepDelay)
	}

	go func() {
		reason, ok := <-r.publishChannel.NotifyClose(make(chan *amqp.Error))

		if !ok {
			log.Println(logTag, "publish channel was normally closed")
			return
		}

		log.Println(logTag, "publish channel was closed. Reason:", reason)

		//delay for re-create
		time.Sleep(sleepDelay)

		r.initPublishChannel()
	}()
}

func (r *RabbitMQVNDelay) dial(url string) error {
	connection, err := amqp.Dial(url)

	if err != nil {
		return err
	}

	r.connection = connection

	go func() {

		for {
			reason, ok := <-r.connection.NotifyClose(make(chan *amqp.Error))

			if !ok {
				log.Println(logTag, "connection was normally closed")
				time.Sleep(sleepDelay)
				break
			}

			log.Println(logTag, "connections was closed. Reason: ", reason)

			//always trying to reconnect
			for {
				r.connection, err = amqp.Dial(url)
				if err == nil {
					log.Println(logTag, "successfull to reconnect")
					break
				}

				log.Println(logTag, "failed to reconnect (will retry). Error: ", err)
				//delay before connect
				time.Sleep(sleepDelay)
			}
		}
	}()

	return nil
}
