package services

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	appError "github.com/fireops-software/fireops-edge-core-gateway/error"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

//--------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------

type CoreNotifier struct {
	ctx             context.Context
	logger          log.ILogger
	rabbitMq        messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi      dal.IFireOpsCoreApi
	excahngeAlu2New messaging.RabbitMqExchange

	retryInterval time.Duration
	retryLimit    int
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

// --------------------------------------------------------------------------------------------
// Private
// --------------------------------------------------------------------------------------------

func (c *CoreNotifier) run() error {
	// Subscibe on rabbitMq
	alu2g := c.rabbitMq.Subscribe(c.excahngeAlu2New)
	defer c.rabbitMq.Unsubscribe(alu2g)
	// Listen and publish
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case msg := <-alu2g:
			if msg.Error != nil {
				return msg.Error
			}
			events := []domain.Event{}
			if err := json.Unmarshal(msg.Result.Body, &events); err != nil {
				return appError.NewErrDataParsing("failed to parse incomming data - %v", err)
			}
			c.logger.Infof("New incomming events from alu2g: %s", string(msg.Result.Body))
			for i := 0; i < c.retryLimit; i++ {
				resp := <-c.fireOpsApi.SendEvents(c.ctx, events)
				if resp.Error != nil {
					c.logger.Errorf("failed to send alu2g events to fireops-core (Retry: %d) - %v", i, resp.Error)
					continue
				}
				c.logger.Infof("Successfully sent new alu2g events to fireops-core api")
				break
			}
		}
	}
}

//--------------------------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------------------------

func NewCoreNotifier(ctx context.Context, logger log.ILogger, rabbitMq messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery], fireopsApi dal.IFireOpsCoreApi, exchangeAlu2gNew messaging.RabbitMqExchange, opts ...func(*CoreNotifier)) *CoreNotifier {
	c := &CoreNotifier{
		ctx:             ctx,
		logger:          logger,
		rabbitMq:        rabbitMq,
		fireOpsApi:      fireopsApi,
		excahngeAlu2New: exchangeAlu2gNew,

		retryInterval: 10 * time.Second,
		retryLimit:    3,
	}
	for _, o := range opts {
		o(c)
	}
	go func() {
		for {
			err := c.run()
			if err == nil {
				// Context exeeded
				break
			}
			logger.Errorf("%v", err)
			time.Sleep(c.retryInterval)
		}
	}()
	return c
}
