package services

import (
	"context"
	"encoding/json"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	appError "github.com/fireops-software/fireops-edge-core-gateway/error"
	"github.com/fireops-software/fireops-edge-core-gateway/utils"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/async"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/health"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

//--------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------

type CoreGateway struct {
	ctx                context.Context
	logger             log.ILogger
	rabbitMq           messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi         dal.IFireOpsCoreApi
	excahngeAlu2Active messaging.RabbitMqExchange
	exchangeActive     messaging.RabbitMqExchange
	exchangeUnits      messaging.RabbitMqExchange

	lastSuccessfullPoll time.Time
	alu2gCache          []domain.Event
	coreCache           []domain.Event

	retryInterval       time.Duration
	fireopsPollInterval time.Duration
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------

func (c *CoreGateway) run() error {
	// Subscribe alu2g
	alu2g := c.rabbitMq.Subscribe(c.excahngeAlu2Active)
	defer c.rabbitMq.Unsubscribe(alu2g)
	// Create ticker for fireops API polling
	ticker := time.NewTicker(c.fireopsPollInterval)

	for {
		select {
		case <-c.ctx.Done():
			// Parent context canceled
			return nil
		case alu2gMsg := <-alu2g:
			if err := c.processAlu2gMsg(alu2gMsg); err != nil {
				return err
			}
		case <-ticker.C:
			// Request from Core api
			coreMsg := <-c.fireOpsApi.GetFireDepState()
			// Process message
			if err := c.processCoreMsg(coreMsg); err != nil {
				return err
			}
			// Store timestamp for healthreport
			c.lastSuccessfullPoll = time.Now()
		}
	}
}

func (c *CoreGateway) processAlu2gMsg(msg async.ActionResult[amqp091.Delivery]) error {
	// Check if predefined error
	if msg.Error != nil {
		return msg.Error
	}
	// Parse message body
	alu2gEvents := []domain.Event{}
	if err := json.Unmarshal(msg.Result.Body, &alu2gEvents); err != nil {
		return appError.NewErrDataParsing("Failed to parse alu2g events - %v", err)
	}
	c.logger.Debugf("New incomming data from Alu2g: %s", utils.MustJsonStr(alu2gEvents))
	// Update cache
	c.alu2gCache = alu2gEvents
	// Notify
	return c.notifyEvents()
}

func (c *CoreGateway) processCoreMsg(msg async.ActionResult[domain.FireDepState]) error {
	// Check if predefined error
	if msg.Error != nil {
		return msg.Error
	}
	// Update EventCache
	c.logger.Debugf("New incomming data from fireops-core: %s", utils.MustJsonStr(msg.Result))
	c.coreCache = msg.Result.Events
	// Notify
	if err := c.notifyEvents(); err != nil {
		return err
	}
	// Notify Units
	return c.rabbitMq.Publish(c.exchangeUnits, msg.Result.Units)
}

func (c *CoreGateway) notifyEvents() error {
	// Merge events
	events := append([]domain.Event{}, c.coreCache...)
	for _, alu2gEvent := range c.alu2gCache {
		if !collections.ContainsSlice(events, func(coreEvent domain.Event) bool {
			return coreEvent.Num1 != nil && alu2gEvent.Num1 != nil && *coreEvent.Num1 == *alu2gEvent.Num1
		}) {
			events = append(events, alu2gEvent)
		}
	}
	// Send to rabbitMq
	return c.rabbitMq.Publish(c.exchangeActive, events)
}

//--------------------------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------------------------

func NewCoreGateway(
	ctx context.Context,
	logger log.ILogger,
	rabbitMq messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery],
	fireOpsApi dal.IFireOpsCoreApi,
	excahngeAlu2Active messaging.RabbitMqExchange,
	exchangeActive messaging.RabbitMqExchange,
	exchangeUnits messaging.RabbitMqExchange,
	opts ...func(*CoreGateway)) *CoreGateway {

	cg := &CoreGateway{
		ctx:                ctx,
		logger:             logger,
		rabbitMq:           rabbitMq,
		fireOpsApi:         fireOpsApi,
		excahngeAlu2Active: excahngeAlu2Active,
		exchangeActive:     exchangeActive,
		exchangeUnits:      exchangeUnits,

		lastSuccessfullPoll: time.Time{},
		alu2gCache:          []domain.Event{},
		coreCache:           []domain.Event{},

		retryInterval:       10 * time.Second,
		fireopsPollInterval: 30 * time.Second,
	}
	for _, o := range opts {
		o(cg)
	}
	// Register healthcheck
	health.GetHealthMonitor().RegisterReadynessCheck("fireops-core-api", func() error {
		if time.Since(cg.lastSuccessfullPoll) > 2*cg.fireopsPollInterval {
			return appError.NewErrFireOpsApi("fireops-core-api not ready")
		}
		return nil
	})
	// Run service
	go func() {
		for {
			err := cg.run()
			if err == nil {
				// Service stopped
				break
			}
			logger.Errorf("%v", err)
			time.Sleep(cg.retryInterval)
		}
	}()
	return cg
}
