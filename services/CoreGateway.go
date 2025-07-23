package services

import (
	"context"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

//--------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------

type CoreGateway struct {
	ctx        context.Context
	logger     log.ILogger
	rabbitMq   messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi dal.IFireOpsCoreApi
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------------------------

func NewCoreGateway(ctx context.Context, logger log.ILogger, rabbitMq messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery], fireOpsApi dal.IFireOpsCoreApi, opts ...func(*CoreGateway)) *CoreGateway {
	cg := &CoreGateway{
		ctx:        ctx,
		logger:     logger,
		rabbitMq:   rabbitMq,
		fireOpsApi: fireOpsApi,
	}
	for _, o := range opts {
		o(cg)
	}
	go func() {
		// TODO: Implement data merge
	}()
	return cg
}
