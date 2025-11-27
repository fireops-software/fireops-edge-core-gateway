package services

import (
	"context"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	"github.com/rabbitmq/amqp091-go"
	"github.com/uoul/go-common/collections"
	"github.com/uoul/go-common/health"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

type HealthReporter struct {
	logger         log.ILogger
	messenger      messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery]
	healthExchange messaging.RabbitMqExchange
	reportInterval time.Duration
	serviceName    string
	displayName    string
	ctx            context.Context
}

func WithHealthReporterInterval(interval time.Duration) func(*HealthReporter) {
	return func(hr *HealthReporter) {
		hr.reportInterval = interval
	}
}

func NewHealthReporter(ctx context.Context, logger log.ILogger, messenger messaging.IMessenger[messaging.RabbitMqExchange, amqp091.Delivery], healthExchange messaging.RabbitMqExchange, serviceName string, displayName string, opts ...func(*HealthReporter)) *HealthReporter {
	h := &HealthReporter{
		logger:         logger,
		messenger:      messenger,
		healthExchange: healthExchange,
		reportInterval: 30 * time.Second,
		serviceName:    serviceName,
		displayName:    displayName,
		ctx:            ctx,
	}
	for _, o := range opts {
		o(h)
	}
	go func() {
		ticker := time.NewTicker(h.reportInterval)
	LP1:
		for {
			select {
			case <-h.ctx.Done():
				break LP1
			case <-ticker.C:
				// Execute Readyness checks
				e := health.GetHealthMonitor().DoReadynessChecks()
				// Evaluate current state
				s := domain.STATE_NOT_READY
				if len(e) <= 0 {
					s = domain.STATE_READY
				}
				currentState := &domain.Health{
					ServiceName: h.serviceName,
					DisplayName: h.displayName,
					Timestamp:   time.Now(),
					State:       s,
					Errors:      collections.MapSlice(e, func(e error) string { return e.Error() }),
					Description: "",
				}
				// Publish
				err := h.messenger.Publish(h.healthExchange, currentState)
				if err != nil {
					h.logger.Errorf("failed to publish current health state - %v", err)
				}
			}
		}
	}()
	return h
}
