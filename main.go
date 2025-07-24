package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/services"
	"github.com/uoul/go-common/config"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

const (
	VERSION      = "{VERSION}"
	SERVICE_NAME = "fireops-edge-core-gateway"
)

func main() {
	// Create AppCtx
	ctx, cancel := context.WithCancel(context.Background())
	// Create ConfigProvider
	cp := config.NewEnvVarProvider()
	// Create Logger
	logger := log.NewConsoleLogger(
		log.StringToLogLevel(
			cp.StringOrDefault("LOG_LEVEL", "INFO"),
			log.INFO,
		),
	)
	// Create RabbitMqClient
	rabbitMq := messaging.NewRabbitMqMessenger(
		ctx,
		logger,
		cp.StringOrDefault("RABBITMQ_HOST", ""),
		cp.UInt16OrDefault("RABBITMQ_PORT", 5672),
		cp.StringOrDefault("RABBITMQ_USER", ""),
		cp.StringOrDefault("RABBITMQ_PW", ""),
	)
	// Create FireOpsCoreApi
	fireopsCoreApi := dal.NewFireOpsCoreApi(
		cp.StringOrDefault("FIREOPS_BASE_URL", ""),
		cp.StringOrDefault("FIREOPS_TOKEN", ""),
	)
	// Run CoreGateway
	alertsExchangeName := cp.StringOrDefault("RABBITMQ_EVENTS_EXCHANGE", "fireops-edge-events")
	services.NewCoreGateway(
		ctx,
		logger,
		rabbitMq,
		fireopsCoreApi,
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ALU2G_ACTIVE_ROUTINGKEY", "alu2g.active"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ACTIVE_ROUTINGKEY", "active"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   cp.StringOrDefault("RABBITMQ_UNITS_EXCHANGE", "fireops-edge-units"),
			RoutingKey: cp.StringOrDefault("RABBITMQ_UNITS_ROUTINGKEY", ""),
		},
	)
	// Run CoreNotifier
	services.NewCoreNotifier(
		ctx,
		logger,
		rabbitMq,
		fireopsCoreApi,
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ALU2G_NEW_ROUTINGKEY", "alu2g.new"),
		},
	)
	// Show run message
	logger.Info("Running...")
	// Wait until stop
	osSig := make(chan os.Signal, 1)
	signal.Notify(osSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-osSig
	cancel()
	logger.Info("Shutting down...")
}
