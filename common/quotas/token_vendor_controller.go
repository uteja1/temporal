package quotas

import (
	"sync"
)

type (
	TokenVendorController interface {
		GetOrCreateTokenVendor(name string, rateFn float64, burstRatioFn float64, priorities int) TokenVendor
	}

	TokenVendorControllerImpl struct {
		tokenVendorMap map[string]TokenVendor
		timeWindow     int
		lock           sync.RWMutex
	}
)

func NewTokenVendorController(tw int) TokenVendorController {
	return &TokenVendorControllerImpl{
		tokenVendorMap: make(map[string]TokenVendor),
		timeWindow:     tw,
	}
}

func (c *TokenVendorControllerImpl) GetOrCreateTokenVendor(
	name string,
	rate float64,
	burstRatio float64,
	priorities int,
) TokenVendor {
	c.lock.RLock()
	if tv, ok := c.tokenVendorMap[name]; ok {
		c.lock.RUnlock()
		return tv
	}
	c.lock.RUnlock()

	c.lock.Lock()
	defer c.lock.Unlock()
	c.tokenVendorMap[name] = NewTokenVendor(priorities, rate, burstRatio, c.timeWindow)
	c.tokenVendorMap[name].Update(rate, burstRatio)
	return c.tokenVendorMap[name]
}
