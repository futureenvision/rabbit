package rabbit

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RabbitClient struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	err        error
}

func (rabbit *RabbitClient) InitConnection(url string) {
	rabbit.connection, rabbit.err = amqp.Dial(url)
	failOnError(rabbit.err, "Failed to connect to RabbitMQ")

	rabbit.channel, rabbit.err = rabbit.connection.Channel()
	failOnError(rabbit.err, "Failed to open a channel")
}

func (rabbit *RabbitClient) InitQueue(queueName string) amqp.Queue {
	queue, err := rabbit.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")
	return queue
}

func (rabbit *RabbitClient) Send(queue amqp.Queue, inData []byte) {
	err := rabbit.channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        inData,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf("[%s][Sent] -> %s\n", queue.Name, string(inData))
}

func (rabbit *RabbitClient) Close() {
	rabbit.connection.Close()
	rabbit.channel.Close()
}
