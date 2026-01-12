package pusbsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{ContentType: "application/json", Body: body}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("conn.Channel returned err: %v", err)
		// TODO: return err
	}

	durable := false
	autoDelete := false
	exclusive := false

	switch queueType {
	case DurableQueue:
		durable = true
	case TransientQueue:
		autoDelete = true
		exclusive = true
	}

	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		log.Fatalf("QueueDeclare returned err: %v", err)
	}

	if err := channel.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		log.Fatalf("QueueBind returned err: %v", err)
	}
	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		log.Printf("DeclareAndBind returned err: %v", err)
	}

	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("channel.Consume returned err: %v", err)
	}

	go func() {
		var target T
		for d := range deliveries {
			if err := json.Unmarshal(d.Body, &target); err != nil {
				log.Printf("json.Unmarshal returned err: %v\n", err)
			}
			handler(target)
			if err := d.Ack(false); err != nil {
				log.Printf("d.Ack returned err: %v\n", err)
			}
		}
	}()

	return nil
}
