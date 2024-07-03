package quotas

import (
	"sync"
	"time"
)

type (
	TokenVendor interface {
		GetTokens(priorityRps map[int32]float32) (int, time.Duration)
	}

	TokenVendorImpl struct {
		rl         *PriorityRateLimiter2Impl
		timeWindow int
		priorities int
		lock       sync.Mutex
	}
)

func NewTokenVendor(priorities int, rate float64, burstRatio float64, timeWindow int) TokenVendor {
	rateLimiters := make(map[int]RateLimiter)
	for i := 0; i < priorities; i++ {
		rateLimiters[i] = NewDynamicRateLimiter(
			NewRateBurst(
				func() float64 {
					return rate
				},
				func() int {
					return int(rate * float64(timeWindow))
				}),
			defaultRefreshInterval)

	}
	rl := NewPriorityRateLimiter2(func(req Request) int {
		return req.Priority
	}, rateLimiters)
	return &TokenVendorImpl{
		rl:         rl,
		priorities: priorities,
		timeWindow: timeWindow,
	}
}

func (tv *TokenVendorImpl) GetTokens(priorityRps map[int32]float32) (int, time.Duration) {
	tv.lock.Lock()
	defer tv.lock.Unlock()
	windowEnd := time.Now().Add(time.Duration(tv.timeWindow) * time.Second)
	tokens := 0
	for p := 0; p < tv.priorities; p++ {
		t := min(tv.rl.TokenAt(windowEnd, p), int(priorityRps[int32(p)]*float32(tv.timeWindow)))
		_ = tv.rl.ReserveP(windowEnd, t, p)
		tokens += t
	}
	return tokens, time.Duration(tv.timeWindow) * time.Second
}
