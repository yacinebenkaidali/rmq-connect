package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	lAMQP "github.com/yacinebenkaidali/rmq-connect/amqp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	configPath := flag.String("config", "amqp.yaml", "path to the config file")
	flag.StringVar(configPath, "c", *configPath, "path to the config file")

	flag.Parse()

	amqpCfg, err := lAMQP.LoadConfigFile(*configPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer cancel()

	conn, err := lAMQP.NewBrokerClient(amqpCfg, ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.RegisterConsumer("consumer01", func(ctx context.Context, msg amqp.Delivery) error {
		log.Println("consumer01, received ", string(msg.Body))
		return nil
	}); err != nil {
		return err
	}

	publisherName := "publisher01"
	publisher := conn.GetPublisher(publisherName)

	if publisher == nil {
		return fmt.Errorf("publisher %s was not registered !", publisherName)
	}

	go func() {
		for range 5 {
			time.Sleep(time.Second * time.Duration(rand.Intn(5)))
			if err := publisher.Publish(
				context.TODO(),
				amqp.Publishing{Body: []byte("dummy test message")},
				publisher.RoutingKey,
			); err != nil {
				log.Printf("failed to publish test message: %v", err)
			}
		}
	}()

	log.Println("consuming")
	<-ctx.Done()
	log.Println("exit signal received, exiting...")
	return nil
}
