package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
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

func NewBrokerClient(ctx context.Context, cfg *AMQP) (*Connection, error) {
	connection := Connection{
		brokerURI:   cfg.BrokerURI,
		ctx:         ctx,
		cfg:         cfg,
		consumers:   make(map[string]*Consumer),
		publishers:  make(map[string]*Publisher),
		consumersWg: sync.WaitGroup{},
		mu:          sync.Mutex{},
	}

	if err := connection.dialConnection(); err != nil {
		return nil, err
	}

	if err := connection.setup(); err != nil {
		var errGrp = errors.Join(err, connection.pubConn.Close(), connection.conConn.Close())
		return nil, errGrp
	}

	go func() {
		for {
			select {
			case err := <-connection.consumerNotifyCloseCh:
				{
					if err == nil {
						log.Printf("amqp consumer close channel closed; stopping reconnect loop for brokerURI=%s", connection.brokerURI)
						return
					}
					if !connection.reconnect() {
						return
					}
					log.Printf("amqp reconnect succeeded: brokerURI=%s", connection.brokerURI)
				}
			case err := <-connection.publisherNotifyCloseCh:
				{
					if err == nil {
						log.Printf("amqp publisher close channel closed; stopping reconnect loop for brokerURI=%s", connection.brokerURI)
						return
					}
					if !connection.reconnect() {
						return
					}
					log.Printf("amqp reconnect succeeded: brokerURI=%s", connection.brokerURI)
				}
			case <-connection.ctx.Done():
				log.Printf("amqp reconnect loop stopped due to context cancellation: brokerURI=%s", connection.brokerURI)
				return
			}
		}
	}()

	return &connection, nil
}

func (c *Connection) dialConnection() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClosed {
		return errClientClosed
	}
	if c.pubConn != nil && !c.pubConn.IsClosed() {
		c.pubConn.Close()
	}

	if c.conConn != nil && !c.conConn.IsClosed() {
		c.conConn.Close()
	}

	conConn, err := amqp.Dial(c.brokerURI)
	if err != nil {
		return err
	}
	c.conConn = conConn

	pubConn, err := amqp.Dial(c.brokerURI)
	if err != nil {
		conConn.Close()
		return err
	}
	c.pubConn = pubConn

	topoCh, err := conConn.Channel()
	if err != nil {
		conConn.Close()
		pubConn.Close()
		return err
	}
	c.topologyCh = topoCh

	var consumerNotifyCloseCh = make(chan *amqp.Error, 1)
	conConn.NotifyClose(consumerNotifyCloseCh)

	var publisherNotifyCloseCh = make(chan *amqp.Error, 1)
	pubConn.NotifyClose(publisherNotifyCloseCh)

	c.consumerNotifyCloseCh = consumerNotifyCloseCh
	c.publisherNotifyCloseCh = publisherNotifyCloseCh

	return nil
}

func (c *Connection) reconnect() bool {
	var timeout = time.Second * 5
	var attempt uint16 = 0

	for {

		select {
		case <-time.After(min(timeout, time.Second*30)):
			{
				timeout = min(timeout*2, 30*time.Second)
				attempt++
				if err := c.dialConnection(); err != nil {
					log.Printf("amqp reconnect failed: brokerURI=%s, attempt=%d, err=%v", c.brokerURI, attempt, err)
					if errors.Is(err, errClientClosed) {
						return false
					}
					continue
				}

				if err := c.resetup(attempt); err != nil {
					log.Printf("amqp reconnect setup failed: brokerURI=%s, attempt=%d, err=%v", c.brokerURI, attempt, err)
					continue
				}
				return true

			}
		case <-c.ctx.Done():
			return false
		}
	}
}

func (c *Connection) setup() error {
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
		ch, err := c.conConn.Channel()
		if err != nil {
			return err
		}

		if existingConsumer, exists := c.consumers[consumer.Name]; exists {
			existingConsumer.conCh = ch
		} else {
			c.consumers[consumer.Name] = &Consumer{
				Name:              consumer.Name,
				conCh:             ch,
				Queue:             consumer.Queue,
				PrefetchCount:     consumer.PrefetchCount,
				MaxFailedAttempts: consumer.MaxFailedAttempts,
			}
		}
	}

	for i := 0; i < len(c.cfg.Publishers); i++ {

		ch, err := c.pubConn.Channel()
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			return err
		}

		if existingPublisher, exists := c.publishers[c.cfg.Publishers[i].Name]; exists {
			existingPublisher.mu.Lock()
			existingPublisher.pubCh = ch
			existingPublisher.mu.Unlock()
		} else {
			c.publishers[c.cfg.Publishers[i].Name] = &Publisher{
				Name:       c.cfg.Publishers[i].Name,
				pubCh:      ch,
				Exchange:   c.cfg.Publishers[i].Exchange,
				RoutingKey: c.cfg.Publishers[i].RoutingKey,
			}
		}

	}

	return nil
}

func (c *Connection) resetup(attempt uint16) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.setup(); err != nil {
		log.Printf("amqp reconnect setup failed: brokerURI=%s, attempt=%d, setupErr=%v", c.brokerURI, attempt, err)
		//close connections
		var errGrp error
		closePubErr := c.pubConn.Close()
		closeConErr := c.conConn.Close()
		errGrp = errors.Join(closePubErr, closeConErr)
		return errGrp
	}

	for _, consumer := range c.consumers {
		if consumer.handler != nil {
			err := consumer.setup(consumer.handler)
			if err != nil {
				return err
			} else {
				c.startConsumer(consumer)
			}
		}
	}
	return nil
}

func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isClosed = true

	var errGrp error
	for _, consumer := range c.consumers {
		if err := consumer.conCh.Cancel(consumer.Name, false); err != nil {
			errGrp = errors.Join(errGrp, err)
		}
	}
	c.consumersWg.Wait()
	if c.conConn != nil && !c.conConn.IsClosed() {
		if err := c.conConn.Close(); err != nil {
			errGrp = errors.Join(errGrp, err)
		}
	}

	if c.pubConn != nil && !c.pubConn.IsClosed() {
		return errors.Join(errGrp, c.pubConn.Close())
	}

	return errGrp
}

func (c *Connection) RegisterConsumer(consumerName string, handler func(ctx context.Context, msg amqp.Delivery) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	consumer, ok := c.consumers[consumerName]
	if !ok {
		return fmt.Errorf("the consumer %s was not declared in the config file", consumerName)
	}

	if consumer.handler != nil {
		return fmt.Errorf("consumer %s: handler already registered", consumer.Name)
	}

	if err := consumer.setup(handler); err != nil {
		return err
	}

	c.startConsumer(consumer)
	return nil
}

func (c *Connection) startConsumer(consumer *Consumer) {
	var con = consumer.conCh
	var channel = consumer.conHandlerCh
	c.consumersWg.Go(func() {
		consumer.consume(c.ctx, con, channel)
	})
}

func (c *Connection) GetPublisher(publisherName string) *Publisher {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishers[publisherName]
}
