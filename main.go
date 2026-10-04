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

	topologyCh *amqp.Channel

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
	name     string
	pubCh    *amqp.Channel
	exchange string
	routing  string
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

	publisherName := "publisher01"
	publisher := conn.Publishers[publisherName]

	if publisher == nil {
		log.Fatal(fmt.Errorf("publisher %s was not registered !", publisherName))
	}

	// go func() {
	for range 5 {
		if err := publisher.pubCh.PublishWithContext(
			conn.Ctx,
			publisher.exchange,
			publisher.routing,
			true,
			false,
			amqp.Publishing{Body: []byte("dummy test message")},
		); err != nil {
			log.Printf("failed to publish test message: %v", err)
		}
	}
	// }()

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

	pubConn, err := amqp.Dial(cfg.BrokerURI)
	if err != nil {
		return nil, err
	}

	topoCh, err := conConn.Channel()
	if err != nil {
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
						if err = consumer.conCh.Reject(msg.DeliveryTag, true); err != nil {
							log.Printf("consumer %s: failed to reject delivery tag %d: %v", consumerName, msg.DeliveryTag, err)
						}
						continue
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

func (c *Connection) Setup() error {
	// exchanges → queues (+ bind) → consumers
	// declaring Exchanges
	for _, e := range c.cfg.Exchanges {
		if err := c.topologyCh.ExchangeDeclare(e.Name, e.Type, false, false, false, false, nil); err != nil {
			return err
		}
	}
	// declaring Queues and their bindings
	for _, q := range c.cfg.Queues {
		_, err := c.topologyCh.QueueDeclare(
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
			name:         consumer.Name,
			conCh:        ch,
			conHandlerCh: msgs,
		}
	}

	for _, publisher := range c.cfg.Publishers {
		ch, err := c.PubConn.Channel()
		if err != nil {
			return err
		}

		c.Publishers[publisher.Name] = &Publisher{
			name:     publisher.Name,
			pubCh:    ch,
			exchange: publisher.Exchange,
			routing:  publisher.RoutingKey,
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
