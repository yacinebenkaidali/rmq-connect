package amqp

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (consumer *Consumer) setup(handler func(ctx context.Context, msg amqp.Delivery) error) error {
	if err := consumer.conCh.Qos(consumer.PrefetchCount, 0, false); err != nil {
		return err
	}

	msgs, err := consumer.conCh.Consume(
		consumer.Queue, // queue
		consumer.Name,  // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		return err
	}

	consumer.handler = handler
	consumer.conHandlerCh = msgs

	return nil
}

func (c *Consumer) consume(ctx context.Context, conCh *amqp.Channel, conHandlerCh <-chan amqp.Delivery) {
	for {
		select {
		case msg, ok := <-conHandlerCh:
			{
				if !ok {
					return
				}
				// recheck this when we support more consumer fields
				err := c.handler(ctx, msg)
				if err != nil {
					log.Printf("consumer %s: handler error: %v", c.Name, err)
					if msg.Headers == nil {
						msg.Headers = make(amqp.Table)
					}
					count := getRetryCountFromHeaders(&msg.Headers)
					count += 1
					if count >= c.MaxFailedAttempts {
						log.Printf("consumer %s: message delivery tag %d exceeded max failed attempts (%d), rejecting", c.Name, msg.DeliveryTag, c.MaxFailedAttempts)
						if err := msg.Reject(false); err != nil {
							log.Printf("consumer %s: failed to reject delivery tag %d: %v", c.Name, msg.DeliveryTag, err)
						}
						continue
					}

					msg.Headers[FAILED_MSG_RETRY_COUNT_HEADER] = count
					if err := conCh.Publish("", c.Queue, false, false, amqp.Publishing{
						Headers:         msg.Headers,
						ContentType:     msg.ContentType,
						ContentEncoding: msg.ContentEncoding,
						DeliveryMode:    msg.DeliveryMode,
						Priority:        msg.Priority,
						CorrelationId:   msg.CorrelationId,
						ReplyTo:         msg.ReplyTo,
						Expiration:      msg.Expiration,
						MessageId:       msg.MessageId,
						Timestamp:       msg.Timestamp,
						Type:            msg.Type,
						UserId:          msg.UserId,
						AppId:           msg.AppId,
						Body:            msg.Body,
					}); err != nil {
						log.Printf("consumer %s: failed to republish delivery tag %d: %v", c.Name, msg.DeliveryTag, err)
						if err := msg.Reject(true); err != nil {
							log.Printf("consumer %s: failed to reject delivery tag %d: %v", c.Name, msg.DeliveryTag, err)
						}
						continue
					}
				}
				log.Printf("acking msg with id %d", msg.DeliveryTag)
				if err = msg.Ack(false); err != nil {
					log.Printf("consumer %s: failed to Ack delivery tag %d: %v", c.Name, msg.DeliveryTag, err)
				}
			}
		case <-ctx.Done():
			{
				return
			}
		}
	}
}
