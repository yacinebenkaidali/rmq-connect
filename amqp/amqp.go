package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

func LoadConfigFile(path string) (*AMQP, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	fileExt := filepath.Ext(path)
	cfg := AMQP{}

	switch fileExt {
	case ".json":
		{
			decoder := json.NewDecoder(fd)
			err = decoder.Decode(&cfg)
		}

	case ".yaml":
		{
			decoder := yaml.NewDecoder(fd)
			err = decoder.Decode(&cfg)
		}
	default:
		return nil, fmt.Errorf("the passed config file has to be either json or yaml")
	}

	if err := validateAMQP(&cfg); err != nil {
		return nil, err
	}
	return &cfg, err
}

func NewConnection(cfg *AMQP, ctx context.Context) (*Connection, error) {

	conConn, err := amqp.Dial(cfg.BrokerURI)
	if err != nil {
		return nil, err
	}

	pubConn, err := amqp.Dial(cfg.BrokerURI)
	if err != nil {
		conConn.Close()
		return nil, err
	}

	topoCh, err := conConn.Channel()
	if err != nil {
		conConn.Close()
		pubConn.Close()
		return nil, err
	}

	connection := Connection{
		Ctx:        ctx,
		ConConn:    conConn,
		cfg:        cfg,
		PubConn:    pubConn,
		topologyCh: topoCh,
		Consumers:  make(map[string]*Consumer),
		Publishers: make(map[string]*Publisher),
	}

	return &connection, err
}

func (c *Connection) Setup() error {
	// exchanges → queues (+ bind) → consumers
	// declaring Exchanges
	for _, e := range c.cfg.Exchanges {
		if err := c.topologyCh.ExchangeDeclare(e.Name, e.Type, e.Durability, false, false, false, nil); err != nil {
			return err
		}
	}
	// declaring Queues and their bindings
	for _, q := range c.cfg.Queues {
		_, err := c.topologyCh.QueueDeclare(
			q.Name,       // name
			q.Durability, // durability
			false,        // delete when unused
			false,        // exclusive
			false,        // no-wait
			amqp.Table{
				amqp.QueueTypeArg: amqp.QueueTypeQuorum,
			},
		)
		if err != nil {
			return err
		}
		err = c.topologyCh.QueueBind(q.Name, q.BindingKey, q.Exchange, false, nil)
		if err != nil {
			return err
		}
	}
	// declaring consumers
	for _, consumer := range c.cfg.Consumers {
		ch, err := c.ConConn.Channel()
		if err != nil {
			return err
		}

		if err := ch.Qos(consumer.PrefetchCount, 0, false); err != nil {
			return err
		}
		msgs, err := ch.Consume(
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

		c.Consumers[consumer.Name] = &Consumer{
			Name:              consumer.Name,
			conCh:             ch,
			conHandlerCh:      msgs,
			Queue:             consumer.Queue,
			PrefetchCount:     consumer.PrefetchCount,
			MaxFailedAttempts: consumer.MaxFailedAttempts,
		}
	}

	for _, publisher := range c.cfg.Publishers {
		ch, err := c.PubConn.Channel()
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			return err
		}

		c.Publishers[publisher.Name] = &Publisher{
			Name:       publisher.Name,
			PubCh:      ch,
			Exchange:   publisher.Exchange,
			RoutingKey: publisher.RoutingKey,
		}
	}

	return nil
}

func (c *Connection) Close() error {
	if c.ConConn != nil && !c.ConConn.IsClosed() {
		if err := c.ConConn.Close(); err != nil {
			return err
		}
	}

	if c.PubConn != nil && !c.PubConn.IsClosed() {
		return c.PubConn.Close()
	}
	return nil
}

func (c *Connection) RegisterConsumer(consumerName string, handler func(ctx context.Context, msg amqp.Delivery) error) error {
	consumer, ok := c.Consumers[consumerName]
	if !ok {
		return fmt.Errorf("the consumer %s was not declared in the config file", consumerName)
	}
	go func() {
		for {
			select {
			case msg, ok := <-consumer.conHandlerCh:
				{
					if !ok {
						return
					}
					// recheck this when we support more consumer fields
					err := handler(c.Ctx, msg)
					if err != nil {
						log.Printf("consumer %s: handler error: %v", consumerName, err)
						if msg.Headers == nil {
							msg.Headers = make(amqp.Table)
						}
						count := getRetryCountFromHeaders(&msg.Headers)
						count += 1
						if count >= consumer.MaxFailedAttempts {
							log.Printf("consumer %s: message delivery tag %d exceeded max failed attempts (%d), rejecting", consumerName, msg.DeliveryTag, consumer.MaxFailedAttempts)
							if err := consumer.conCh.Reject(msg.DeliveryTag, false); err != nil {
								log.Printf("consumer %s: failed to reject delivery tag %d: %v", consumerName, msg.DeliveryTag, err)
							}
							continue
						}

						msg.Headers[FAILED_MSG_RETRY_COUNT_HEADER] = count
						if err := consumer.conCh.Publish("", consumer.Queue, false, false, amqp.Publishing{
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
							log.Printf("consumer %s: failed to republish delivery tag %d: %v", consumerName, msg.DeliveryTag, err)
							if err := consumer.conCh.Reject(msg.DeliveryTag, true); err != nil {
								log.Printf("consumer %s: failed to reject delivery tag %d: %v", consumerName, msg.DeliveryTag, err)
							}
							continue
						}
					}
					log.Printf("acking msg with id %d", msg.DeliveryTag)
					if err = consumer.conCh.Ack(msg.DeliveryTag, false); err != nil {
						log.Printf("consumer %s: failed to Ack delivery tag %d: %v", consumerName, msg.DeliveryTag, err)
					}
				}
			case <-c.Ctx.Done():
				{
					return
				}
			}
		}
	}()
	return nil
}

func (p *Publisher) Publish(ctx context.Context, msg amqp.Publishing, rKey string) error {
	// the provided routingKey takes precedance over the publisher's default routingKey
	var routingKey string
	if rKey == "" {
		if p.RoutingKey == "" {
			return fmt.Errorf("publisher's default routingKey and the provided routingKey are both empty !")
		}
		routingKey = p.RoutingKey
	} else {
		routingKey = rKey
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	confirmation, err := p.PubCh.PublishWithDeferredConfirmWithContext(ctx, p.Exchange, routingKey, false, false, msg)
	if err != nil {
		return err
	}
	ok, err := confirmation.WaitContext(timeoutCtx)
	if err != nil {
		return fmt.Errorf("confirm not received: %w", err)
	}
	if !ok {
		return fmt.Errorf("message nacked by broker")
	}
	return err
}
