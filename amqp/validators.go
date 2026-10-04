package amqp

import "fmt"

func validateExchange(e *Exchange, i int) error {
	switch e.Type {
	case "direct", "fanout", "topic", "headers":
	default:
		return fmt.Errorf("unknown type of exchange %d", i)
	}
	if e.Name == "" {
		return fmt.Errorf("exchange %d name is required", i)
	}
	return nil
}

func validateQueue(q *Queue, i int, exchanges map[string]string) error {
	if q.Name == "" {
		return fmt.Errorf("queue %d name is required", i)
	}
	if q.Exchange == "" {
		return fmt.Errorf("queue's exchange %d name is required", i)
	}
	var exchangeType string
	var ok bool
	if exchangeType, ok = exchanges[q.Exchange]; !ok {
		return fmt.Errorf("queue's exchange %s at %d is not declared", q.Exchange, i)
	}
	if (exchangeType == "direct" || exchangeType == "topic") && q.BindingKey == "" {
		return fmt.Errorf("queue's bindingKey %d is required", i)
	}

	return nil
}

func validateConsumer(c *Consumer, i int, queues map[string]struct{}) error {
	if c.Name == "" {
		return fmt.Errorf("consumer %d name is required", i)
	}
	if c.Queue == "" {
		return fmt.Errorf("consumer %d queue name is required", i)
	}
	if c.PrefetchCount < 0 {
		return fmt.Errorf("consumer %d prefetch can not be less than 0", i)
	} else if c.PrefetchCount == 0 {
		// using the default pretech count when it is not provided (optional)
		c.PrefetchCount = DEFAULT_PREFETCH_COUNT
	}
	if c.MaxFailedAttempts < 0 {
		return fmt.Errorf("consumer %d failed retry count can not be less than 0", i)
	} else if c.MaxFailedAttempts == 0 {
		// using the default message retry count when it is not provided (optional)
		c.MaxFailedAttempts = DEFAULT_FAILED_MSG_RETRIES
	}
	if _, ok := queues[c.Queue]; !ok {
		return fmt.Errorf("consumer's queue %s at %d is not declared", c.Queue, i)
	}
	return nil
}

func validatePublisher(publisher *Publisher, i int, exchanges map[string]string) error {
	if publisher.Name == "" {
		return fmt.Errorf("publisher %d name is required", i)
	}
	if publisher.Exchange == "" {
		return fmt.Errorf("publisher %d exchange name is required", i)
	}
	if _, ok := exchanges[publisher.Exchange]; !ok {
		return fmt.Errorf("publisher's exchange %s is not declared !", publisher.Exchange)
	}
	return nil
}

func validateAMQP(cfg *AMQP) error {
	if cfg.BrokerURI == "" {
		return fmt.Errorf("broker URI is required to connect to the AMQP broker")
	}
	if len(cfg.Consumers) == 0 {
		return fmt.Errorf("consumers are required for the configuration")
	}
	if len(cfg.Queues) == 0 {
		return fmt.Errorf("queues are required for the configuration")
	}
	if len(cfg.Exchanges) == 0 {
		return fmt.Errorf("exchanges are required for the configuration")
	}

	exchanges := make(map[string]string) // name ==> type
	queues := make(map[string]struct{})
	consumers := make(map[string]struct{})
	publishers := make(map[string]struct{})

	for i, e := range cfg.Exchanges {
		if _, ok := exchanges[e.Name]; ok {
			return fmt.Errorf("exchange %s declared more than once", e.Name)
		}
		if err := validateExchange(&e, i); err != nil {
			return err
		}
		exchanges[e.Name] = e.Type
	}

	for i, q := range cfg.Queues {
		if _, ok := queues[q.Name]; ok {
			return fmt.Errorf("queue %s declared more than once", q.Name)
		}
		if err := validateQueue(&q, i, exchanges); err != nil {
			return err
		}
		queues[q.Name] = struct{}{}
	}

	for i := 0; i < len(cfg.Consumers); i++ {
		c := cfg.Consumers[i]
		if _, ok := consumers[c.Name]; ok {
			return fmt.Errorf("consumer %s declared more than once", c.Name)
		}
		if err := validateConsumer(&cfg.Consumers[i], i, queues); err != nil {
			return err
		}
		consumers[c.Name] = struct{}{}
	}

	for i, pub := range cfg.Publishers {
		if _, ok := publishers[pub.Name]; ok {
			return fmt.Errorf("publisher %s declared more than once", pub.Name)
		}

		if err := validatePublisher(&pub, i, exchanges); err != nil {
			return err
		}
		publishers[pub.Name] = struct{}{}

	}

	return nil
}
