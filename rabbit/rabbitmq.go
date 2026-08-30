package rabbit

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueName    = "webhook_deliveries"
	exchangeName = ""
)

var retryQueues = []struct {
	name string
	ttl  int
}{
	{QueueName + ".retry.30s", 30000},
	{QueueName + ".retry.1m", 60000},
	{QueueName + ".retry.5m", 300000},
	{QueueName + ".retry.15m", 900000},
	{QueueName + ".retry.30m", 1800000},
	{QueueName + ".retry.1h", 3600000},
}

type RabbitMQ struct {
	conn       *amqp.Connection
	publishCh  *amqp.Channel
	consumeCh  *amqp.Channel
}

func New(amqpURL string) (*RabbitMQ, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < 30; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		log.Printf("[rabbitmq] connect attempt %d/30 failed: %v", i+1, err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	publishCh, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open publish channel: %w", err)
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		publishCh.Close()
		conn.Close()
		return nil, fmt.Errorf("open consume channel: %w", err)
	}

	if err := declareQueues(publishCh); err != nil {
		publishCh.Close()
		consumeCh.Close()
		conn.Close()
		return nil, err
	}

	if err := consumeCh.Qos(1, 0, false); err != nil {
		publishCh.Close()
		consumeCh.Close()
		conn.Close()
		return nil, fmt.Errorf("set qos: %w", err)
	}

	return &RabbitMQ{
		conn:      conn,
		publishCh: publishCh,
		consumeCh: consumeCh,
	}, nil
}

func declareQueues(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(
		QueueName,
		true, false, false, false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": retryQueues[0].name,
		},
	)
	if err != nil {
		return fmt.Errorf("declare main queue: %w", err)
	}

	for i, rq := range retryQueues {
		nextKey := QueueName
		if i < len(retryQueues)-1 {
			nextKey = retryQueues[i+1].name
		}

		_, err := ch.QueueDeclare(
			rq.name,
			true, false, false, false,
			amqp.Table{
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": nextKey,
				"x-message-ttl":             rq.ttl,
			},
		)
		if err != nil {
			return fmt.Errorf("declare retry queue %s: %w", rq.name, err)
		}
	}

	return nil
}

func (r *RabbitMQ) Publish(body []byte) error {
	return r.publishCh.Publish(
		exchangeName,
		QueueName,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Timestamp:    time.Now().UTC(),
		},
	)
}

func (r *RabbitMQ) PublishToRetryQueue(body []byte, retryCount int) error {
	queueName := QueueName
	if retryCount > 0 && retryCount <= len(retryQueues) {
		queueName = retryQueues[retryCount-1].name
	} else if retryCount > len(retryQueues) {
		queueName = retryQueues[len(retryQueues)-1].name
	}

	return r.publishCh.Publish(
		exchangeName,
		queueName,
		false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Timestamp:    time.Now().UTC(),
		},
	)
}

func (r *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	return r.consumeCh.Consume(
		QueueName,
		"",
		false, false, false, false,
		nil,
	)
}

func (r *RabbitMQ) Ack(tag uint64, multiple bool) error {
	return r.consumeCh.Ack(tag, multiple)
}

func (r *RabbitMQ) Nack(tag uint64, multiple bool, requeue bool) error {
	return r.consumeCh.Nack(tag, multiple, requeue)
}

func (r *RabbitMQ) Close() {
	if r.publishCh != nil {
		r.publishCh.Close()
	}
	if r.consumeCh != nil {
		r.consumeCh.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
