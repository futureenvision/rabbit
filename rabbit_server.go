package main

import (
	"log"

	"github.com/streadway/amqp"
)

type onQueue func(body []byte)
type RabbitServer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	err  error
}

func (rabbit *RabbitServer) InitConnection(url string) {
	rabbit.conn, rabbit.err = amqp.Dial(url)
	failOnError(rabbit.err, "Failed to connect to RabbitMQ")

	rabbit.ch, rabbit.err = rabbit.conn.Channel()
	failOnError(rabbit.err, "Failed to open a channel")
}

func (rabbit *RabbitServer) On(queueName string, function onQueue) {
	queue, err := rabbit.ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	messagesChan, err := rabbit.ch.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range messagesChan {
			log.Printf("[%s][Received] -> %s\n", queue.Name, d.Body)
			function(d.Body)
		}
	}()
	log.SetFlags(0)
	log.Printf("[Rabbit Servering ... %s]\n", queueName)
}

func (rabbit *RabbitServer) Close() {
	rabbit.conn.Close()
	rabbit.ch.Close()
}
