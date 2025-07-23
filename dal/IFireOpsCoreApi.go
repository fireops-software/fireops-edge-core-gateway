package dal

import (
	"github.com/fireops-software/fireops-edge-core-gateway/domain"
	"github.com/uoul/go-common/async"
)

type IFireOpsCoreApi interface {
	SendEvents([]domain.Event) chan async.ActionResult[any]
	GetFireDepState() chan async.ActionResult[domain.FireDepState]
}
