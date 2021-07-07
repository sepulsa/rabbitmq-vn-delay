# rabbitmq-vn-delay

Lightweight library to send and subscribe queue, with delayed feature using rabbitMQ native plugins.
This library also handling auto reconnect for connection and channel (wrap inside)

## Feature Overview

-   Send queue with delay
-   Send queue directly, without delay
-   Subscribe multiple queue with specified total of worker for each

## Getting Started

### Installation

    go get github.com/sepulsa/rabbitmq-vn-delay

## Example

#### Publish Directly Without Delay

    package main

    import (
    	r "github.com/sepulsa/rabbitmq-vn-delay"
    	"github.com/streadway/amqp"
    )

    func main() {
    	url := "amqp://guest:guest@localhost:5672/"

    	rabbitMQ, err := r.NewRabbitMQVNDelay(url)
    	if err != nil {
    		panic(err)
    	}

    	err = rabbitMQ.Publish("demo", "this is just demo")
    	if err != nil {
    		panic(err)
    	}
    }

#### Publish With Delay Feature

    package main

    import (
    	"time"

    	r "github.com/sepulsa/rabbitmq-vn-delay"
    	"github.com/streadway/amqp"
    )

    func main() {
    	url := "amqp://guest:guest@localhost:5672/"

    	rabbitMQ, err := r.NewRabbitMQVNDelay(url)
    	if err != nil {
    		panic(err)
    	}

    	err = rabbitMQ.PublishWithDelay("demo", "this is just demo", time.Second*5)
    	if err != nil {
    		panic(err)
    	}
    }

#### Subcribe Queue

    package main

    import (
        "sync"
    	"time"
        "log"

    	r "github.com/sepulsa/rabbitmq-vn-delay"
    	"github.com/streadway/amqp"
    )

    func main() {
        var waitgroup sync.WaitGroup
    	url := "amqp://guest:guest@localhost:5672/"

    	rabbitMQ, err := r.NewRabbitMQVNDelay(url)
    	if err != nil {
    		panic(err)
    	}
        
        handler := func(data string, ack r.AckFn) {
		    log.Printf("Data: %s\n", data)

            //mark task as complete
    		ack()
            
            waitgroup.Done()
	    }

        //register the subscriber
        rabbitMQ.Subscribe("demo", 1, handler)

        waitgroup.Add(count)
        
        //start worker
    	rabbitMQ.Start()
        
	    waitgroup.Wait()
    }
