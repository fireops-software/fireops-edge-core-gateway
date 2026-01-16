package dal

import (
	"context"
	"time"

	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	"github.com/uoul/go-common/async"
)

type IFireOpsCoreApi interface {
	SendHealthReport(ctx context.Context, report []domain.Health) chan async.ActionResult[any]
	SendEvents(ctx context.Context, events []domain.Event) chan async.ActionResult[any]
	GetFireDepState(ctx context.Context) chan async.ActionResult[domain.FireDepState]
	GetWaterExtractionPoints(ctx context.Context, radius float32, lastUpdate time.Time) <-chan async.ActionResult[[]domain.WaterExtractionPoint]
}
