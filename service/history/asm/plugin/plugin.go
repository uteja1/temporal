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
	"context"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Plugin interface {
		Type() string

		Configs() Configs
		TaskCategories() []tasks.Category
		// TaskExecutor() TaskExecutor
		// TaskVerifier() TaskVerifier // replication only
		Serializer() Serializer

		Start(context.Context, StartInstanceRequest) (StartInstanceResponse, error)
		Restore(context.Context, RestoreInstanceRequest) (RestoreInstanceResponse, error)
		// Reset(context.Context, ResetInstanceRequest) (ResetInstanceResponse, error)
	}

	Configs struct {
		// DisableVisibility  bool
		// DisablePersistence        bool
		// DisableReplication        bool
		// DisableHistoryArchival    bool
		// DisableVisibilityArchival bool
	}

	Instance interface {
		Transit(context.Context, TransitInstanceRequest) (TransitInstanceResponse, error)

		// IsRunning() bool // execution state
		// IsFailed() bool  // execution close status

		// Key() definition.WorkflowKey
		StateSnapshot() StateSnapshot // part of state that needs to be persisted, it's ok to have in memory only state
		// SearchAttributes() SearchAttributes

		// we can make this more granular if needed
		// like IDRetention, VisibilityRetention, HistoryRetention etc.
		// Retention() time.Duration
	}
)

type (
	StateSnapshot struct {
		Core any

		// subStateType -> (subStateID -> subState)
		// SubStates map[string]map[string]any
	}

	// StateMutation struct {
	// 	Core any

	// 	// subStateType -> (subStateID -> subState)
	// 	UpsertSubStates map[string]map[string]any
	// 	DeleteSubStates map[string]map[string]struct{}
	// }

	// TODO: specify the list of accepted search attribute value types
	SearchAttributes map[string]interface{}
)

type (
	StartInstanceRequest struct {
		Key       definition.WorkflowKey
		Resources Resources

		ASMStartRequest any
	}

	StartInstanceResponse struct {
		NewInstanceTransition InstanceTransition

		ASMStartResponse any
	}

	RestoreInstanceRequest struct {
		StateSnapshot StateSnapshot
	}

	RestoreInstanceResponse struct {
		Instance Instance
	}

	InstanceMetadata struct {
		// StartTime time.Time
		// CloseTime time.Time

		HistoryEventCount int64
		// HistoryEventBytes    int64
		// StateTransitionCount int64
		// StateSnapshotBytes   int64

		// add more here if needed
	}

	Resources struct {
		Metadata                   InstanceMetadata
		HistoryEventTokenGenerator HistoryEventTokenGenerator
		HistoryEventsIterator      HistoryEventsIterator
		HistoryEventLoader         HistoryEventLoader

		Logger         log.Logger
		MetricsHandler metrics.Handler
	}

	TransitInstanceRequest struct {
		RequestASMToken []byte
		CurrentASMToken []byte
		// NewASMToken     ASMToken
		Resources Resources

		// this will be the key for the new instance if NewInstanceStartRequest is specified in the response
		// why do we need this? Current run want to know the runID of the new run.
		NewInstanceKey definition.WorkflowKey

		Event Event
	}

	TransitInstanceResponse struct {
		CurrentInstanceTransition *InstanceTransition

		// if not nil, a new instance will be created (via ASM.Start())
		NewInstanceStartRequest any

		ASMTransitResponse any
	}
)

type (
	InstanceTransition struct {
		// Instance object after the transition
		// this can be the same object as the one used to make the transition
		Instance Instance

		// optional: if Mutation is returned, it will be used to update persisted state of the ASM
		// implementation need to make sure after Mutation is applied, state in DB matches the state in memory
		// Mutation *StateMutation

		HistoryEvents []HistoryEvent
		Tasks         []Task

		// default: false
		// Ignored if DisableVisibility is true when registering the ASM
		UpsertVisibility bool

		// default: false
		// if true, Mutation, HistoryEvents & Tasks must be empty. UpsertVisibility must be false
		// Ignored if DisablePersistence is true when registering the ASM
		// SkipPersistence bool
	}
)
