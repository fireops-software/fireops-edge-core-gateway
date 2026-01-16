package services

import (
	"context"
	"encoding/json"
	"sync"
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

type CoreEventGateway struct {
	ctx            context.Context
	logger         log.ILogger
	rabbitMq       messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi     dal.IFireOpsCoreApi
	exchangeAlu2g  messaging.RabbitMqExchange
	exchangeActive messaging.RabbitMqExchange
	exchangeNew    messaging.RabbitMqExchange
	exchangeUnits  messaging.RabbitMqExchange
	waterMapClient *WaterMapClient

	lastSuccessfullPoll time.Time
	alu2gCache          []domain.Event
	coreCache           []domain.Event
	mux                 sync.Mutex
	eventBuffer         *utils.RingBuffer[string]

	retryInterval              time.Duration
	fireopsPollInterval        time.Duration
	waterExtractionPointRadius float64
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------

func (c *CoreEventGateway) run() error {
	// Subscribe alu2g
	alu2g := c.rabbitMq.Subscribe(c.exchangeAlu2g)
	defer c.rabbitMq.Unsubscribe(alu2g)
	// Create ticker once
	ticker := time.NewTicker(c.fireopsPollInterval)
	defer ticker.Stop()
	// Run service
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
			coreMsg := <-c.fireOpsApi.GetFireDepState(c.ctx)
			// Process message
			if err := c.processCoreMsg(coreMsg); err != nil {
				return err
			}
			// Store timestamp for healthreport
			c.lastSuccessfullPoll = time.Now()
		}
	}
}

func (c *CoreEventGateway) processAlu2gMsg(msg async.ActionResult[amqp091.Delivery]) error {
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
	c.notifyEvents(c.ctx)
	return nil
}

func (c *CoreEventGateway) processCoreMsg(msg async.ActionResult[domain.FireDepState]) error {
	// Check if predefined error
	if msg.Error != nil {
		return msg.Error
	}
	// Update EventCache
	c.logger.Debugf("New incomming data from fireops-core: %s", utils.MustJsonStr(msg.Result))
	c.coreCache = msg.Result.Events
	// Notify
	c.notifyEvents(c.ctx)
	// Notify Units
	return c.rabbitMq.Publish(c.exchangeUnits, msg.Result.Units)
}

func (c *CoreEventGateway) notifyEvents(ctx context.Context) {
	c.mux.Lock()
	defer c.mux.Unlock()
	// Merge events
	events := append([]domain.Event{}, c.coreCache...)
	for _, alu2gEvent := range c.alu2gCache {
		if !collections.ContainsSlice(events, func(coreEvent domain.Event) bool {
			return coreEvent.Num1 != nil && alu2gEvent.Num1 != nil && *coreEvent.Num1 == *alu2gEvent.Num1
		}) {
			events = append(events, alu2gEvent)
		}
	}
	// Enrich even with water extraction points
	for i, e := range events {
		if e.Num1 != nil && e.Latitude != nil && e.Longitude != nil {
			wep, err := c.waterMapClient.GetWaterExtractionPoints(ctx, *e.Latitude, *e.Longitude, c.waterExtractionPointRadius)
			if err != nil {
				c.logger.Warningf("failed to get water extraction points for %s - %v", *e.Num1, err)
			}
			events[i].WaterExtractionPoints = wep
		}
	}
	// Check for new events
	//newEvents := collections.FilterSlice(events, func(e domain.Event) bool {
	//	if e.Num1 == nil || c.eventBuffer.Contains(*e.Num1) {
	//		return false
	//	}
	//	c.eventBuffer.Push(*e.Num1)
	//	return e.FullChain != nil && *e.FullChain
	//})
	//newEventIds := collections.MapSlice(newEvents, func(e domain.Event) string {
	//	if e.Num1 != nil {
	//		return *e.Num1
	//	} else {
	//		return ""
	//	}
	//})
	//if len(newEvents) > 0 {
	//	// Notify FireOPS
	//	fireOpsResponse := c.fireOpsApi.SendEvents(c.ctx, newEvents)
	//	// Publish on rabbitmq
	//	if err := c.rabbitMq.Publish(c.exchangeNew, newEvents); err != nil {
	//		c.logger.Error(err.Error())
	//	}
	//	c.logger.Debugf("Successfully published new events (%v) on rabbitmq", newEventIds)
	//	// Wait for fireops response
	//	if r := <-fireOpsResponse; r.Error != nil {
	//		c.logger.Error(r.Error.Error())
	//	}
	//	c.logger.Debugf("Successfully notified fireops about new events (%v)", newEventIds)
	//}
	// Send active events to rabbitMq
	if err := c.rabbitMq.Publish(c.exchangeActive, events); err != nil {
		c.logger.Error(err.Error())
	}
	c.logger.Debugf("Successfully published active events on rabbitmq")
}

func WithCoreGatewayEventRadius(r uint) func(*CoreEventGateway) {
	return func(ceg *CoreEventGateway) {
		ceg.waterExtractionPointRadius = float64(r) / 1000.0
	}
}

//--------------------------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------------------------

func NewCoreEventGateway(
	ctx context.Context,
	logger log.ILogger,
	rabbitMq messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery],
	fireOpsApi dal.IFireOpsCoreApi,
	exchangeAlu2g messaging.RabbitMqExchange,
	exchangeActive messaging.RabbitMqExchange,
	exchangeNew messaging.RabbitMqExchange,
	exchangeUnits messaging.RabbitMqExchange,
	waterMapClient *WaterMapClient,
	opts ...func(*CoreEventGateway)) *CoreEventGateway {

	cg := &CoreEventGateway{
		ctx:            ctx,
		logger:         logger,
		rabbitMq:       rabbitMq,
		fireOpsApi:     fireOpsApi,
		exchangeAlu2g:  exchangeAlu2g,
		exchangeActive: exchangeActive,
		exchangeNew:    exchangeNew,
		exchangeUnits:  exchangeUnits,
		waterMapClient: waterMapClient,

		lastSuccessfullPoll: time.Time{},
		alu2gCache:          []domain.Event{},
		coreCache:           []domain.Event{},
		mux:                 sync.Mutex{},
		eventBuffer:         utils.NewRingBuffer[string](50),

		retryInterval:              10 * time.Second,
		fireopsPollInterval:        30 * time.Second,
		waterExtractionPointRadius: 1.0,
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
