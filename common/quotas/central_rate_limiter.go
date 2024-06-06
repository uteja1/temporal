package quotas

import (
	"sync"
	"time"
)

//type (
//	// RequestRateLimiterFn returns generate a namespace specific rate limiter
//	RequestRateLimiterFn func(req Request) RequestRateLimiter
//
//	// RequestPriorityFn returns a priority for the given Request
//	RequestPriorityFn func(req Request) int
//
//	// RequestRateLimiter corresponds to basic rate limiting functionality.
//	RequestRateLimiter interface {
//		// Allow attempts to allow a request to go through. The method returns
//		// immediately with a true or false indicating if the request can make
//		// progress
//		Allow(now time.Time, request Request) bool
//
//		// Reserve returns a Reservation that indicates how long the caller
//		// must wait before event happen.
//		Reserve(now time.Time, request Request) Reservation
//
//		// Wait waits till the deadline for a rate limit token to allow the request
//		// to go through.
//		Wait(ctx context.Context, request Request) error
//	}
//)

type (
	CentralRateLimiter interface {
		GetTokens(int) (int, time.Duration)
	}
	CentralRateLimiterImpl struct {
		rateLimiter *DynamicRateLimiterImpl
		epochFn     func() int
		mutex       sync.Mutex
	}
)

func NewCentralRateLimiter(rateFn RateFn, epochFn func() int) CentralRateLimiter {
	return &CentralRateLimiterImpl{
		rateLimiter: NewDynamicRateLimiter(
			NewRateBurst(
				rateFn,
				func() int {
					return epochFn() * int(rateFn())
				}),
			time.Duration(epochFn())*time.Second),
		epochFn: epochFn,
	}
}

func (c *CentralRateLimiterImpl) GetTokens(rps int) (int, time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	tokensNeeded := rps * c.epochFn()
	// Not allowing burst now
	tokensAfterEpoch := int(c.rateLimiter.TokensAt(time.Now().Add(time.Duration(c.epochFn()) * time.Second)))
	tokensAllocated := min(tokensNeeded, tokensAfterEpoch)
	c.rateLimiter.ReserveN(time.Now(), tokensAllocated)
	return tokensAllocated, 5 * time.Second
}
