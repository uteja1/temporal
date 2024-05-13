package quotas

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
)

type (
	DistributedRateLimiter struct {
		tokens        int
		expiryTime    time.Time
		mutex         sync.Mutex
		historyClient historyservice.HistoryServiceClient
	}
	DistributedReservation struct {
		tokens  int
		limiter *DistributedRateLimiter
	}
)

var _ RequestRateLimiter = (*DistributedRateLimiter)(nil)
var _ Reservation = (*DistributedReservation)(nil)

func (d *DistributedRateLimiter) Allow(now time.Time, request Request) bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.expiryTime.After(time.Now()) {
		d.tokens = 0
	}
	if d.tokens < request.Token {
		resp, err := d.historyClient.ReserveRateLimiterTokens(context.Background(), &historyservice.ReserveRateLimiterTokensRequest{
			Requester: request.Caller,
			Tokens:    100,
			ShardId:   int32(hashStringTo2048(request.Caller)),
		})
		{
		}
		if err != nil {
			// LOG ERROR
			return false
		}
		d.tokens += int(resp.GetTokenss())
		d.expiryTime = time.Now()
	}
	if d.tokens >= request.Token {
		d.tokens -= request.Token
		return true
	}
	return false
}

func (d *DistributedRateLimiter) Reserve(now time.Time, request Request) Reservation {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.expiryTime.After(time.Now()) {
		d.tokens = 0
	}

	if d.tokens < request.Token {
		resp, err := d.historyClient.ReserveRateLimiterTokens(context.Background(), &historyservice.ReserveRateLimiterTokensRequest{
			Requester: request.Caller,
			Tokens:    100,
			ShardId:   int32(hashStringTo2048(request.Caller)),
		})
		if err != nil {
			// LOG ERROR
			// Just allowing this request for now.
			return NoopReservation
		}
		d.tokens += int(resp.GetTokenss())
		d.expiryTime = time.Now().Add(time.Second)
	}
	if d.tokens >= request.Token {
		d.tokens -= request.Token
		return &DistributedReservation{
			tokens:  request.Token,
			limiter: d,
		}
	}
	return &DistributedReservation{
		tokens:  0,
		limiter: d,
	}
}

func (d *DistributedRateLimiter) Wait(ctx context.Context, reqeust Request) error {
	return nil
}

func (d *DistributedRateLimiter) Cancel(tokens int) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.tokens += tokens
}

func hashStringTo2048(s string) int {
	// Create a new FNV hash instance.
	h := fnv.New32()

	// Write the string to the hash.
	h.Write([]byte(s))

	// Get the hash value.
	hashValue := h.Sum32()

	// Use modulo operation to restrict the range to [0, 2048).
	return int(hashValue % 512)
}

// OK returns whether the limiter can provide the requested number of tokens
func (r *DistributedReservation) OK() bool {
	if r.tokens > 0 {
		return true
	}
	return false
}

// Cancel indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *DistributedReservation) Cancel() {
	if r.tokens > 0 {
		r.limiter.Cancel(r.tokens)
	}
}

// CancelAt indicates that the reservation holder will not perform the reserved action
// and reverses the effects of this Reservation on the rate limit as much as possible
func (r *DistributedReservation) CancelAt(_ time.Time) {
	panic("Not implemented")
}

// Delay returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *DistributedReservation) Delay() time.Duration {
	if r.tokens > 0 {
		return time.Duration(0) // no delay
	}
	return time.Duration(InfDuration)
}

// DelayFrom returns the duration for which the reservation holder must wait
// before taking the reserved action.  Zero duration means act immediately.
func (r *DistributedReservation) DelayFrom(_ time.Time) time.Duration {
	// if r.tokens > 0 {
	// 	return time.Duration(0) // no delay
	// }
	// return time.Duration(InfDuration)
	panic("Not Implemented")
}
