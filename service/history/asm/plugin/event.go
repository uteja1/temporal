// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package plugin

import (
	"time"
)

type (
	Event interface {
		EventType() EventType
		EventTime() time.Time
	}

	EventType int

	TransitEvent struct {
		Timestamp time.Time
		Request   any
	}

	ExecuteTaskEvent struct {
		Timestamp time.Time
		Task      any
	}

	// ApplyHistoryEvent struct {
	// 	HistoryEvent HistoryEvent
	// }

	// SyncSnapshotEvent struct {
	// 	State StateSnapshot
	// }

	// SyncMutationEvent struct {
	// 	StateMutation StateMutation
	// }

	// TerminateEvent struct {
	// 	Reason   string
	// 	Details  common.Payload
	// 	Identity string
	// }
)

const (
	EventTypeTransit EventType = iota
	EventTypeExecuteTask
	// EventTypeApplyHistory
	// EventTypeSyncSnapshot // replication only
	// EventTypeSyncMutation // replication only
	// EventTypeTerminate
)

func NewTransitEvent(
	timestamp time.Time,
	request any,
) *TransitEvent {
	return &TransitEvent{
		Timestamp: timestamp,
		Request:   request,
	}
}

func (e *TransitEvent) EventType() EventType {
	return EventTypeTransit
}

func (e *TransitEvent) EventTime() time.Time {
	return e.Timestamp
}

func NewExecuteTaskEvent(
	timestamp time.Time,
	task any,
) *ExecuteTaskEvent {
	return &ExecuteTaskEvent{
		Timestamp: timestamp,
		Task:      task,
	}
}

func (e *ExecuteTaskEvent) EventType() EventType {
	return EventTypeExecuteTask
}

func (e *ExecuteTaskEvent) EventTime() time.Time {
	return e.Timestamp
}
