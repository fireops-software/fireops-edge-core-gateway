package services

import (
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

type CoreHealthNotifier struct {
	ctx            context.Context
	logger         log.ILogger
	rabbitMq       messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	fireOpsApi     dal.IFireOpsCoreApi
	healthExchange messaging.RabbitMqExchange

	retryInterval time.Duration
	retryLimit    int

	lastUpdateSent             time.Time
	cyclicNotificationInterval time.Duration
	cache                      map[string]domain.Health
}

func (c *CoreHealthNotifier) updateServiceState(serviceHealth domain.Health) bool {
	serviceHealthCache, exists := c.cache[serviceHealth.ServiceName]
	if !exists {
		c.cache[serviceHealth.ServiceName] = serviceHealth
		return true
	}
	if serviceHealthCache.State != serviceHealth.State || !reflect.DeepEqual(serviceHealthCache.Errors, serviceHealth.Errors) {
		c.cache[serviceHealth.ServiceName] = serviceHealth
		return true
	}
	return false
}

func (c *CoreHealthNotifier) sendHealthReport() {
	for i := 0; i < c.retryLimit; i++ {
		resp := <-c.fireOpsApi.SendHealthReport(c.ctx, slices.Collect(maps.Values(c.cache)))
		if resp.Error != nil {
			c.logger.Errorf("failed to send health report to fireops-core (Retry: %d) - %v", i, resp.Error)
			time.Sleep(c.retryInterval)
			continue
		}
		c.logger.Infof("Successfully sent health report to fireops-core api")
		c.lastUpdateSent = time.Now()
		break
	}
}

func (c *CoreHealthNotifier) run() error {
	// Subscribe to health exchange
	healthSrc := c.rabbitMq.Subscribe(c.healthExchange)
	defer c.rabbitMq.Unsubscribe(healthSrc)
	// Listen for health messages
	for {
		select {
		case <-c.ctx.Done():
			// Stop - Context dead
			return nil
		case msg := <-healthSrc:
			if msg.Error != nil {
				return msg.Error
			}
			// Parse data
			serviceHealth := domain.Health{}
			if err := json.Unmarshal(msg.Result.Body, &serviceHealth); err != nil {
				c.logger.Errorf("failed to parse incomming data as health message - %v", err)
				continue
			}
			// Send Notification if status has changed
			if c.updateServiceState(serviceHealth) {
				c.sendHealthReport()
			}
		case <-time.Tick(c.cyclicNotificationInterval):
			if time.Since(c.lastUpdateSent) > c.cyclicNotificationInterval {
				c.sendHealthReport()
			}
		}
	}
}

func NewCoreHealthNotifier(ctx context.Context, logger log.ILogger, rabbitMq messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery], fireOpsApi dal.IFireOpsCoreApi, healthExchange messaging.RabbitMqExchange, cyclicNotificationInterval time.Duration, opts ...func(*CoreHealthNotifier)) *CoreHealthNotifier {
	c := &CoreHealthNotifier{
		ctx:            ctx,
		logger:         logger,
		rabbitMq:       rabbitMq,
		fireOpsApi:     fireOpsApi,
		healthExchange: healthExchange,
		lastUpdateSent: time.Time{},

		cyclicNotificationInterval: cyclicNotificationInterval,
		cache:                      map[string]domain.Health{},
		retryInterval:              30 * time.Second,
		retryLimit:                 3,
	}
	for _, o := range opts {
		o(c)
	}
	// Run application
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
