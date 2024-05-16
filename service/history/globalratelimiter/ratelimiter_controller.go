package globalratelimiter

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
)

type (
	Controller interface {
		GetNamespaceRateLimiter(ns string) quotas.RequestRateLimiter
	}

	ControllerImpl struct {
		mutex          sync.Mutex
		rateLimiters   sync.Map
		logger         log.Logger
		metricsHandler metrics.Handler
		config         *configs.Config
	}
)

func ControllerProvider(
	config *configs.Config,
	logger log.Logger,
	metricsHandler metrics.Handler,
) Controller {
	return &ControllerImpl{
		mutex:          sync.Mutex{},
		rateLimiters:   sync.Map{},
		logger:         logger,
		metricsHandler: metricsHandler,
		config:         config,
	}
}

func (c *ControllerImpl) GetNamespaceRateLimiter(ns string) quotas.RequestRateLimiter {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if limiter, ok := c.rateLimiters.Load(ns); ok {
		return limiter.(quotas.RequestRateLimiter)
	}
	c.rateLimiters.Store(
		ns,
		quotas.NewRequestRateLimiterAdapter(
			quotas.NewDefaultIncomingRateLimiter(func() float64 {
				return float64(c.config.NamespaceAPS(ns))
			}),
		))
	rl, _ := c.rateLimiters.Load(ns)
	return rl.(quotas.RequestRateLimiter)
}
