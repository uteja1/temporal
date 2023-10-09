package chasm

import (
	"context"
	"time"
)

type (
	HistoryEvent interface {
		EventID() int64
		EventTime() time.Time

		// hint for if this event should be cached
		ShouldCache() bool
		ShouldReapply() bool
	}

	HistoryEventToken []byte
)

type (
	HistoryEventsIterator interface {
		Next() (HistoryEvent, error)
		HasNext() bool
	}
)

type (
	HistoryEventTokenGenerator interface {
		Generate(eventID int) HistoryEventToken
	}
)

type (
	HistoryEventLoader interface {
		Load(context.Context, LoadHistoryEventRequest) (LoadHistoryEventResponse, error)

		// TODO: add more methods here
		// e.g. RangeLoad, ReverseRangeLoad,RangeLoadRaw, etc.
	}

	LoadHistoryEventRequest struct {
		HistoryEventToken HistoryEventToken
	}

	LoadHistoryEventResponse struct {
		HistoryEvent HistoryEvent
	}
)
