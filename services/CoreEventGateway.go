package services

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	appError "github.com/fireops-software/fireops-edge-core-gateway/error"
	"github.com/fireops-software/fireops-edge-core-gateway/utils"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-collections/slices"
	"github.com/uoul/go-common/async"
	"github.com/uoul/go-common/health"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

//--------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------

type CoreEventGateway struct {
	logger         log.ILogger
	rabbitMq       messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi     dal.IFireOpsCoreApi
	exchangeAlu2g  messaging.RabbitMqExchange
	exchangeActive messaging.RabbitMqExchange
	exchangeNew    messaging.RabbitMqExchange
	exchangeUnits  messaging.RabbitMqExchange

	lastSuccessfullPoll time.Time
	alu2gMux            sync.RWMutex
	alu2gCache          []domain.Event
	coreMux             sync.RWMutex
	coreCache           []domain.Event
	eventBuffer         *utils.RingBuffer[string]

	activeEventsMux sync.RWMutex
	activeEvents    []domain.Event

	retryInterval       time.Duration
	fireopsPollInterval time.Duration
}

//--------------------------------------------------------------------------------------------
// Public
//--------------------------------------------------------------------------------------------

//--------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------

func (c *CoreEventGateway) run(ctx context.Context) error {
	// Subscribe alu2g
	alu2g := c.rabbitMq.Subscribe(c.exchangeAlu2g)
	defer c.rabbitMq.Unsubscribe(alu2g)
	// Create ticker once
	ticker := time.NewTicker(c.fireopsPollInterval)
	defer ticker.Stop()
	// Run service
	for {
		select {
		case <-ctx.Done():
			// Parent context canceled
			return nil
		case alu2gMsg := <-alu2g:
			if err := c.processAlu2gMsg(ctx, alu2gMsg); err != nil {
				return err
			}
		case <-ticker.C:
			// Request from Core api
			coreMsg := <-c.fireOpsApi.GetFireDepState(ctx)
			// Process message
			if err := c.processCoreMsg(ctx, coreMsg); err != nil {
				return err
			}
			// Store timestamp for healthreport
			c.lastSuccessfullPoll = time.Now()
		}
	}
}

func (c *CoreEventGateway) processAlu2gMsg(ctx context.Context, msg async.ActionResult[amqp091.Delivery]) error {
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
	// Check if alu2g events has changed
	c.alu2gMux.RLock()
	eventsChanged := !reflect.DeepEqual(alu2gEvents, c.alu2gCache)
	c.alu2gMux.RUnlock()
	// Update cache
	c.alu2gMux.Lock()
	c.alu2gCache = alu2gEvents
	c.alu2gMux.Unlock()
	// Notify FireOPS if somthing has changed
	if eventsChanged {
		// Start Request to FireOPS
		fireopsResponseCh := c.fireOpsApi.SendEvents(ctx, alu2gEvents)
		// Notify locally (rabbitmq)
		c.notifyEvents()
		// Wait for FireOPS response
		fireopsResponse := <-fireopsResponseCh
		return fireopsResponse.Error
	} else {
		c.notifyEvents()
		return nil
	}
}

func (c *CoreEventGateway) processCoreMsg(ctx context.Context, msg async.ActionResult[domain.FireDepState]) error {
	// Check if predefined error
	if msg.Error != nil {
		return msg.Error
	}
	// Update EventCache
	c.logger.Debugf("New incomming data from fireops-core: %s", utils.MustJsonStr(msg.Result))
	c.coreMux.Lock()
	c.coreCache = msg.Result.Events
	c.coreMux.Unlock()
	// Notify
	c.notifyEvents()
	// Notify Units
	return c.rabbitMq.Publish(c.exchangeUnits, msg.Result.Units)
}

func mergeEvents(l1 []domain.Event, l2 []domain.Event) []domain.Event {
	// Copy l1 as starting point (Filter all with valid eventId - num1)
	r := slices.Filter(l1, func(e domain.Event) bool { return e.Num1 != nil })
	// Merge not included events from l2
	for _, l2Event := range l2 {
		if l2Event.Num1 != nil && !slices.Contains(r, func(e domain.Event) bool { return *l2Event.Num1 == *e.Num1 }) {
			r = append(r, l2Event)
		}
	}
	// Return merged events
	return r
}

func (c *CoreEventGateway) notifyEvents() {
	// Merge events
	c.coreMux.RLock()
	c.alu2gMux.RLock()
	merged := mergeEvents(c.coreCache, c.alu2gCache)
	c.alu2gMux.RUnlock()
	c.coreMux.RUnlock()
	// Filter new events
	newEvents := []domain.Event{}
	for _, e := range merged {
		if e.FullChain != nil && *e.FullChain && e.Num1 != nil && !c.eventBuffer.Contains(*e.Num1) {
			// Add new event to buffer
			c.eventBuffer.Push(*e.Num1)
			// Add new event to new events
			newEvents = append(newEvents, e)
		}
	}
	// Handle new events
	if len(newEvents) > 0 {
		// Publish on rabbitmq
		if err := c.rabbitMq.Publish(c.exchangeNew, newEvents); err != nil {
			c.logger.Error(err.Error())
		}
		c.logger.Debugf("Successfully published new events on rabbitmq: %s", strings.Join(slices.Map(newEvents, func(e domain.Event) string { return *e.Num1 }), ", "))
	}
	// Send active events to rabbitMq
	if err := c.rabbitMq.Publish(c.exchangeActive, merged); err != nil {
		c.logger.Error(err.Error())
	}
	c.logger.Debugf("Successfully published active events on rabbitmq")
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
	opts ...func(*CoreEventGateway)) *CoreEventGateway {

	cg := &CoreEventGateway{
		logger:         logger,
		rabbitMq:       rabbitMq,
		fireOpsApi:     fireOpsApi,
		exchangeAlu2g:  exchangeAlu2g,
		exchangeActive: exchangeActive,
		exchangeNew:    exchangeNew,
		exchangeUnits:  exchangeUnits,

		lastSuccessfullPoll: time.Time{},
		alu2gCache:          []domain.Event{},
		alu2gMux:            sync.RWMutex{},
		coreCache:           []domain.Event{},
		coreMux:             sync.RWMutex{},
		activeEvents:        []domain.Event{},
		activeEventsMux:     sync.RWMutex{},
		eventBuffer:         utils.NewRingBuffer[string](50),

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
			err := cg.run(ctx)
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
