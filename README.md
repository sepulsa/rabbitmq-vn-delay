# rabbitmq-vn-delay

Lightweight library to send queue, with delayed feature using rabbitMQ native plugins.

## Feature Overview

-   Send queue with delay
-   Send queue directly, without delay

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
    	conn, err := amqp.Dial(url)
    	if err != nil {
    		panic(err)
    	}

    	publisher, err := r.NewRabbitMQVNDelay(conn)
    	if err != nil {
    		panic(err)
    	}

    	err = publisher.Publish("demo", "this is just demo")
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
    	conn, err := amqp.Dial(url)
    	if err != nil {
    		panic(err)
    	}

    	publisher, err := r.NewRabbitMQVNDelay(conn)
    	if err != nil {
    		panic(err)
    	}

    	err = publisher.PublishWithDelay("demo", "this is just demo", time.Second*5)
    	if err != nil {
    		panic(err)
    	}
    }
