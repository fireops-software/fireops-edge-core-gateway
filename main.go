package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/dal"
	"github.com/fireops-software/fireops-edge-core-gateway/services"
	"github.com/redis/go-redis/v9"
	"github.com/uoul/go-common/config"
	"github.com/uoul/go-common/log"
	"github.com/uoul/go-common/messaging"
)

const (
	VERSION      = "{VERSION}"
	SERVICE_NAME = "fireops-edge-core-gateway"
	DISPLAY_NAME = "FireOPS"
)

func main() {
	// Create AppCtx
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()
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
	// Create redis clinet
	redisDb := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf(
			"%s:%d",
			cp.StringOrDefault("REDIS_HOST", "localhost"),
			cp.Int16OrDefault("REDIS_PORT", 6379),
		),
		Username: cp.StringOrDefault("REDIS_USER", ""),
		Password: cp.StringOrDefault("REDIS_PW", ""),
		DB:       cp.IntOrDefault("REDIS_DB", 0),
	})
	// Run WaterMapClient
	waterMapClient := services.NewWaterMapClient(
		ctx,
		logger,
		fireopsCoreApi,
		redisDb,
		services.WithWaterMapClientCacheRadius(cp.UIntOrDefault("WATERMAP_CACHE_RADIUS", 20000)),
	)
	// Run CoreGateway
	alertsExchangeName := cp.StringOrDefault("RABBITMQ_EVENTS_EXCHANGE", "fireops-edge-events")
	services.NewCoreEventGateway(
		ctx,
		logger,
		rabbitMq,
		fireopsCoreApi,
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ALU2G_ROUTINGKEY", "alu2g"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_ACTIVE_ROUTINGKEY", "active"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   alertsExchangeName,
			RoutingKey: cp.StringOrDefault("RABBITMQ_NEW_ROUTINGKEY", "new"),
		},
		messaging.RabbitMqExchange{
			Type:       "topic",
			Exchange:   cp.StringOrDefault("RABBITMQ_UNITS_EXCHANGE", "fireops-edge-units"),
			RoutingKey: cp.StringOrDefault("RABBITMQ_UNITS_ROUTINGKEY", ""),
		},
		waterMapClient,
		services.WithCoreGatewayEventRadius(cp.UIntOrDefault("WATERMAP_EVENT_RADIUS", 500)),
	)
	// Define health exchange
	healthExchange := messaging.RabbitMqExchange{
		Type:       "topic",
		Exchange:   cp.StringOrDefault("RABBITMQ_HEALTH_EXCHANGE", "fireops-edge-health"),
		RoutingKey: cp.StringOrDefault("RABBITMQ_HEALTH_ROUTING_KEY", ""),
	}
	// Run CoreHealthNotifier
	services.NewCoreHealthNotifier(
		ctx,
		logger,
		rabbitMq,
		fireopsCoreApi,
		healthExchange,
		time.Duration(cp.UIntOrDefault("CORE_HEALTH_SEND_INTERVAL", 3600))*time.Second,
	)
	// Run HealthReporter
	services.NewHealthReporter(
		ctx,
		logger,
		rabbitMq,
		healthExchange,
		SERVICE_NAME,
		DISPLAY_NAME,
	)

	// Wait until stop
	logger.Info("Running...")
	<-ctx.Done()
	logger.Info("Shutting down...")
}
