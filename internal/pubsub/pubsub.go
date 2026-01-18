package pusbsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

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
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, msg); err != nil {
		return fmt.Errorf("couldn't publish: %v", err)
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	gobEncoder := gob.NewEncoder(&buffer)

	err := gobEncoder.Encode(val)
	if err != nil {
		return fmt.Errorf("can't encode: %v", err)
	}

	msg := amqp.Publishing{ContentType: "application/gob", Body: buffer.Bytes()}
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, msg); err != nil {
		return fmt.Errorf("couldn't publish gob: %v", err)
	}

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

	table := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}
	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
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
	handler func(T) AckType,
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
			ackType := handler(target)
			switch ackType {
			case Ack:
				if err := d.Ack(false); err != nil {
					log.Printf("Ack error: %v\n", err)
				}
				log.Println("Ack")
			case NackRequeue:
				if err := d.Nack(false, true); err != nil {
					log.Printf("NackRequeue error: %v\n", err)
				}
				log.Println("NackRequeue")
			case NackDiscard:
				if err := d.Nack(false, false); err != nil {
					log.Printf("NackDiscard error: %v\n", err)
				}
				log.Println("NackDiscard")
			}
		}
	}()

	return nil
}
