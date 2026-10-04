package amqp

type AMQP struct {
	BrokerURI string `yaml:"brokerURI" json:"brokerURI"`

	Exchanges  []Exchange  `yaml:"exchanges" json:"exchanges"`
	Queues     []Queue     `yaml:"queues" json:"queues"`
	Consumers  []Consumer  `yaml:"consumers" json:"consumers"`
	Publishers []Publisher `yaml:"publishers" json:"publisher"`
}
type Exchange struct {
	Name string `yaml:"name" json:"name"`
	Type string `yaml:"type" json:"type"`
}
type Queue struct {
	Name       string `yaml:"name" json:"name"`
	Exchange   string `yaml:"exchange" json:"exchange"`
	BindingKey string `yaml:"bindingKey" json:"bindingKey"`
}
type Consumer struct {
	Name  string `yaml:"name" json:"name"`
	Queue string `yaml:"queue" json:"queue"`
}
type Publisher struct {
	Name       string `yaml:"name" json:"name"`
	Exchange   string `yaml:"exchange" json:"exchange"`
	RoutingKey string `yaml:"routingKey" json:"routingKey"`
}
