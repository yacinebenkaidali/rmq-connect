package main

import (
	"flag"
	"log"

	amqp "github.com/yacinebenkaidali/rmq-connect/amqp"
)

func main() {
	configPath := flag.String("config", "amqp.yaml", "path to the config file")
	flag.StringVar(configPath, "c", *configPath, "path to the config file")

	flag.Parse()

	amqpCfg, err := amqp.LoadConfigFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(amqpCfg.BrokerURI)

	// ctx := context.Background()
	// env := rmq.NewEnvironment(brokerURI, nil)
	// conn, err := env.NewConnection(ctx)
	// if err != nil {
	// 	log.Panicf("Failed to connect to RabbitMQ: %v", err)
	// }

	// defer func() {
	// 	_ = env.CloseConnections(context.Background())
	// }()

	// _ = conn
}
