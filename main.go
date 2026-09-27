package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	lAMQP "github.com/yacinebenkaidali/rmq-connect/amqp"
)

type Connection struct {
	Ctx      context.Context
	Conn     *amqp.Connection
	Channel  *amqp.Channel
	Handlers map[string]<-chan amqp.Delivery

	cfg *lAMQP.AMQP
}

func main() {
	configPath := flag.String("config", "amqp.yaml", "path to the config file")
	flag.StringVar(configPath, "c", *configPath, "path to the config file")

	flag.Parse()

	var forever chan struct{}

	amqpCfg, err := lAMQP.LoadConfigFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := Connect(amqpCfg, context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	//declare queues first, then consumers
	if err := conn.Setup(); err != nil {
		log.Fatal(err)
	}

	conn.Register("consumer01", func(ctx context.Context, msg amqp.Delivery) error {
		log.Println("consumer01, received ", string(msg.Body))
		return nil
	})

	log.Println("consuming")
	<-forever

}

func Connect(cfg *lAMQP.AMQP, ctx context.Context) (*Connection, error) {
	conn, err := amqp.Dial(cfg.BrokerURI)
	if err != nil {
		return nil, err
	}
	connection := Connection{
		Ctx:      ctx,
		Conn:     conn,
		cfg:      cfg,
		Handlers: make(map[string]<-chan amqp.Delivery),
	}

	go func() {
		<-ctx.Done()
		connection.Close()
	}()

	ch, err := connection.Conn.Channel()
	if err != nil {
		return nil, err
	}
	connection.Channel = ch

	return &connection, err
}

func (c *Connection) Register(consumer string, handler func(ctx context.Context, msg amqp.Delivery) error) error {
	msgs, ok := c.Handlers[consumer]
	if !ok {
		return fmt.Errorf("the consumer %s was not declared in the config file", consumer)
	}
	go func() {
		for msg := range msgs {
			handler(c.Ctx, msg)
		}
	}()
	return nil
}

func (c *Connection) Setup() error {

	for _, exchange := range c.cfg.Exchanges {
		if err := c.Channel.ExchangeDeclare(exchange.Name, exchange.Type, false, false, false, false, nil); err != nil {
			return err
		}
	}

	for _, q := range c.cfg.Queues {
		_, err := c.Channel.QueueDeclare(
			q.Name, // name
			true,   // durability
			false,  // delete when unused
			false,  // exclusive
			false,  // no-wait
			amqp.Table{
				amqp.QueueTypeArg: amqp.QueueTypeQuorum,
			},
		)
		if err != nil {
			return err
		}
	}

	for _, consumer := range c.cfg.Consumers {
		msgs, err := c.Channel.Consume(
			consumer.Queue, // queue
			consumer.Name,  // consumer
			true,           // auto-ack
			false,          // exclusive
			false,          // no-local
			false,          // no-wait
			nil,            // args
		)
		if err != nil {
			return err
		}

		c.Handlers[consumer.Name] = msgs
	}
	return nil
}

func (c *Connection) Close() error {
	return c.Conn.Close()
}
