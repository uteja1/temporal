package quotas

import (
	"sync"
	"time"
)

type (
	TokenVendor interface {
		GetTokens(priorityRps map[int32]float32) (int, time.Duration)
		Update(rate float64, burstRatio float64)
	}

	TokenVendorImpl struct {
		rl         *PriorityRateLimiter2Impl
		timeWindow int
		priorities int
		rate       float64
		burstRatio float64
		lock       sync.Mutex
	}
)

func NewTokenVendor(priorities int, rate float64, burstRatio float64, timeWindow int) TokenVendor {
	tv := &TokenVendorImpl{
		priorities: priorities,
		timeWindow: timeWindow,
		rate:       rate,
		burstRatio: burstRatio,
	}
	rateLimiters := make(map[int]RateLimiter)
	for i := 0; i < priorities; i++ {
		rateLimiters[i] = NewDynamicRateLimiter(
			NewRateBurst(
				func() float64 {
					return tv.rate
				},
				func() int {
					return int(tv.rate * float64(tv.timeWindow) * tv.burstRatio)
				}),
			defaultRefreshInterval)

	}
	tv.rl = NewPriorityRateLimiter2(func(req Request) int {
		return req.Priority
	}, rateLimiters)
	return tv
}

func (tv *TokenVendorImpl) Update(rate float64, burstRatio float64) {
	tv.rate = rate
	tv.burstRatio = burstRatio
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
