package amqp

import (
	"context"

	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (p *Publisher) Publish(ctx context.Context, msg amqp.Publishing, rKey string) error {
	// the provided routingKey takes precedance over the publisher's default routingKey
	var routingKey string
	if rKey == "" {
		routingKey = p.RoutingKey
	} else {
		routingKey = rKey
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	p.mu.Lock()
	pubCh := p.pubCh
	p.mu.Unlock()
	confirmation, err := pubCh.PublishWithDeferredConfirmWithContext(ctx, p.Exchange, routingKey, false, false, msg)
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
