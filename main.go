package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	lAMQP "github.com/yacinebenkaidali/rmq-connect/amqp"
)

type Connection struct {
	Ctx     context.Context
	PubConn *amqp.Connection // publisher connection
	ConConn *amqp.Connection // consumer connection

	Consumers  map[string]*Consumer
	Publishers map[string]*Publisher

	cfg *lAMQP.AMQP
}

type Consumer struct {
	name         string
	conHandlerCh <-chan amqp.Delivery
	conCh        *amqp.Channel
}

type Publisher struct {
	name  string
	conCh *amqp.Channel
}

func main() {
	configPath := flag.String("config", "amqp.yaml", "path to the config file")
	flag.StringVar(configPath, "c", *configPath, "path to the config file")

	flag.Parse()

	amqpCfg, err := lAMQP.LoadConfigFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)

	conn, err := Connect(amqpCfg, ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	//declare queues first, then consumers
	if err := conn.Setup(); err != nil {
		log.Fatal(err)
	}

	if err := conn.RegisterConsumer("consumer01", func(ctx context.Context, msg amqp.Delivery) error {
		log.Println("consumer01, received ", string(msg.Body))
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// var forever chan struct{}

	defer cancel()

	log.Println("consuming")
	<-ctx.Done()
	log.Println("exit signal received, exiting...")

}

func Connect(cfg *lAMQP.AMQP, ctx context.Context) (*Connection, error) {
	conConn, err := amqp.Dial(cfg.BrokerURI)
	if err != nil {
		return nil, err
	}
	connection := Connection{
		Ctx:        ctx,
		ConConn:    conConn,
		cfg:        cfg,
		PubConn:    nil, //TODO
		Consumers:  make(map[string]*Consumer),
		Publishers: make(map[string]*Publisher),
	}

	return &connection, err
}

func (c *Connection) RegisterConsumer(consumerName string, handler func(ctx context.Context, msg amqp.Delivery) error) error {
	consumer, ok := c.Consumers[consumerName]
	if !ok {
		return fmt.Errorf("the consumer %s was not declared in the config file", consumer.name)
	}
	go func() {
		for msg := range consumer.conHandlerCh {
			handler(c.Ctx, msg)
		}
	}()
	return nil
}

func (c *Connection) Setup() error {
	consumers := make(map[string]string) // queue -> consumer
	queues := make(map[string]string)    // exchange -> queue

	// declaring consumers
	for _, consumer := range c.cfg.Consumers {
		ch, err := c.ConConn.Channel()
		if err != nil {
			return err
		}
		msgs, err := ch.Consume(
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

		consumers[consumer.Queue] = consumer.Name

		c.Consumers[consumer.Name] = &Consumer{
			name:         consumer.Name,
			conHandlerCh: msgs,
			conCh:        ch,
		}
	}

	// declaring Queues
	for _, q := range c.cfg.Queues {
		conName := consumers[q.Name]
		consumer := c.Consumers[conName]

		_, err := consumer.conCh.QueueDeclare(
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
		err = consumer.conCh.QueueBind(q.Name, q.BindingKey, q.Exchange, false, nil)
		if err != nil {
			return err
		}
		queues[q.Exchange] = q.Name
	}

	// declaring Exchanges
	for _, e := range c.cfg.Exchanges {
		q := queues[e.Name]
		conName := consumers[q]
		consumer := c.Consumers[conName]

		if err := consumer.conCh.ExchangeDeclare(e.Name, e.Type, false, false, false, false, nil); err != nil {
			return err
		}
	}

	return nil
}

func (c *Connection) Close() error {
	if c.ConConn != nil && !c.ConConn.IsClosed() {
		err := c.ConConn.Close()
		if err != nil {
			return err
		}
	}

	if c.PubConn != nil && !c.PubConn.IsClosed() {
		return c.PubConn.Close()
	}
	return nil
}
