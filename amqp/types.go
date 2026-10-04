package amqp

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQP struct {
	BrokerURI string `yaml:"brokerURI" json:"brokerURI"`

	Exchanges  []Exchange  `yaml:"exchanges" json:"exchanges"`
	Queues     []Queue     `yaml:"queues" json:"queues"`
	Consumers  []Consumer  `yaml:"consumers" json:"consumers"`
	Publishers []Publisher `yaml:"publishers" json:"publishers"`
}
type Exchange struct {
	Name       string `yaml:"name" json:"name"`
	Type       string `yaml:"type" json:"type"`
	Durability bool   `yaml:"durability" json:"durability"`
}
type Queue struct {
	Name       string `yaml:"name" json:"name"`
	Exchange   string `yaml:"exchange" json:"exchange"`
	BindingKey string `yaml:"bindingKey" json:"bindingKey"`
	Durability bool   `yaml:"durability" json:"durability"`
}
type Consumer struct {
	Name          string `yaml:"name" json:"name"`
	Queue         string `yaml:"queue" json:"queue"`
	PrefetchCount int    `yaml:"prefetchCount" json:"prefetchCount"`

	conHandlerCh <-chan amqp.Delivery
	conCh        *amqp.Channel
}
type Publisher struct {
	Name       string `yaml:"name" json:"name"`
	Exchange   string `yaml:"exchange" json:"exchange"`
	RoutingKey string `yaml:"routingKey" json:"routingKey"`

	PubCh *amqp.Channel
}
type Connection struct {
	Ctx     context.Context
	PubConn *amqp.Connection // publisher connection
	ConConn *amqp.Connection // consumer connection

	topologyCh *amqp.Channel

	Consumers  map[string]*Consumer
	Publishers map[string]*Publisher

	cfg *AMQP
}
