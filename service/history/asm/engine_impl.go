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

package asm

import (
	"context"

	"github.com/google/uuid"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	engineImpl struct {
		shardController shard.Controller
		asmCache        Cache
		pluginRegistry  Registry
	}
)

func NewEngine(
	shardController shard.Controller,
	asmCache Cache,
	pluginRegistry Registry,
) Engine {
	return &engineImpl{
		shardController: shardController,
		asmCache:        asmCache,
		pluginRegistry:  pluginRegistry,
	}
}

func (e *engineImpl) Start(
	ctx context.Context,
	request StartRequest,
) (StartResponse, error) {

	if request.IDReusePolicy != IDReusePolicyRejectDuplicate {
		return StartResponse{}, serviceerror.NewUnimplemented("only IDReusePolicyRejectDuplicate is supported")
	}

	shardContext, err := e.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(request.NamespaceID),
		request.ID,
	)
	if err != nil {
		return StartResponse{}, err
	}

	pluginImpl := e.pluginRegistry.Get(request.ASMType)
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.NamespaceID))
	if err != nil {
		return StartResponse{}, err
	}
	asmKey := definition.NewWorkflowKey(
		request.NamespaceID,
		request.ID,
		uuid.New().String(),
	)

	mutableState := NewMutableState(
		shardContext,
		pluginImpl,
		pluginImpl.Type(),
		asmKey,
		request.RequestID,
		namespaceEntry,
		shardContext.GetTimeSource().Now(),
	)

	startResponse, err := pluginImpl.Start(ctx, plugin.StartInstanceRequest{
		Key: asmKey,
		Resources: plugin.Resources{
			Metadata: plugin.InstanceMetadata{
				HistoryEventCount: mutableState.Execution().NextEventId - 1,
			},
			HistoryEventTokenGenerator: newHistoryEventTokenGenerator(
				mutableState.Execution().NextEventId,
				mutableState.CurrentVersion(),
			),
			// HistoryEventLoader: newHistoryEventLoader(
			// 	shardContext.GetEventsCache(),
			// 	asmKey,
			// 	mutableState.GetCurrentBranchToken(),
			// 	pluginImpl.Serializer(),
			// ),
			Logger:         shardContext.GetLogger(),
			MetricsHandler: shardContext.GetMetricsHandler(),
		},
		ASMStartRequest: request.ASMStartRequest,
	})
	if err != nil {
		return StartResponse{}, err
	}

	if err := applyTransitionToMutableState(
		asmKey,
		startResponse.NewInstanceTransition,
		mutableState,
	); err != nil {
		return StartResponse{}, err
	}

	mutableStateTransition, err := mutableState.CloseTransition()
	if err != nil {
		return StartResponse{}, err
	}

	_, err = shardContext.UpsertASM(
		ctx,
		&persistence.UpsertASMRequest{
			ShardID: shardContext.GetShardID(),
			ASMTransitions: []persistence.ASMTransition{
				mutableStateTransition,
			},
			CurrentASMTransition: &persistence.CurrentASMTransition{
				ASMKey:            asmKey,
				ExecutionMetadata: mutableState.ExecutionMetadata(),
				PreviousRunID:     "",
			},
		},
	)
	if err != nil {
		// TODO: need to handle conflict error here for IDReusePolicy
		return StartResponse{}, err
	}

	newToken, err := mutableState.Token()
	if err != nil {
		return StartResponse{}, err
	}

	return StartResponse{
		ASMToken:         newToken,
		ASMStartResponse: startResponse.ASMStartResponse,
	}, nil
}

func (e *engineImpl) Transit(
	ctx context.Context,
	request TransitRequest,
) (_ TransitResponse, retError error) {

	shardContext, err := e.shardController.GetShardByNamespaceWorkflow(
		namespace.ID(request.ASMRef.NamespaceID),
		request.ASMRef.ID,
	)
	if err != nil {
		return TransitResponse{}, err
	}

	asmContext, releaseFn, err := e.asmCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		definition.NewWorkflowKey(
			request.ASMRef.NamespaceID,
			request.ASMRef.ID,
			request.ASMRef.RunID,
		),
		LockPriorityHigh,
	)
	if err != nil {
		return TransitResponse{}, err
	}
	defer func() { releaseFn(retError) }()

	mutableState, err := asmContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		return TransitResponse{}, err
	}

	if err := mutableState.ValidateToken(request.ASMToken); err != nil {
		return TransitResponse{}, err
	}

	currentToken, err := mutableState.Token()
	if err != nil {
		return TransitResponse{}, err
	}
	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return TransitResponse{}, err
	}
	transitResp, err := mutableState.PluginInstance().Transit(
		ctx,
		plugin.TransitInstanceRequest{
			RequestASMToken: request.ASMToken,
			CurrentASMToken: currentToken,
			Resources: plugin.Resources{
				Metadata: plugin.InstanceMetadata{
					HistoryEventCount: mutableState.Execution().NextEventId - 1,
				},
				HistoryEventTokenGenerator: newHistoryEventTokenGenerator(
					mutableState.Execution().NextEventId,
					mutableState.CurrentVersion(),
				),
				HistoryEventsIterator: newHistoryEventsIterator(
					shardContext.GetShardID(),
					currentBranchToken,
					shardContext.GetExecutionManager(),
					mutableState.Plugin().Serializer(),
				),
				HistoryEventLoader: newHistoryEventLoader(
					shardContext.GetShardID(),
					shardContext.GetEventsCache(),
					asmContext.GetKey(),
					currentBranchToken,
					mutableState.Plugin().Serializer(),
				),
				Logger:         shardContext.GetLogger(),
				MetricsHandler: shardContext.GetMetricsHandler(),
			},
			NewInstanceKey: definition.NewWorkflowKey(
				request.ASMRef.NamespaceID,
				request.ASMRef.ID,
				uuid.New().String(),
			),
			Event: plugin.NewTransitEvent(
				shardContext.GetTimeSource().Now(),
				request.ASMTransitRequest,
			),
		},
	)
	if err != nil {
		return TransitResponse{}, err
	}

	if transitResp.NewInstanceStartRequest != nil {
		return TransitResponse{}, serviceerror.NewUnimplemented("NewInstanceStartRequest is not supported yet")
	}

	transition := transitResp.CurrentInstanceTransition
	if transition == nil {
		newToken, err := mutableState.Token()
		if err != nil {
			return TransitResponse{}, err
		}

		return TransitResponse{
			ASMToken:           newToken,
			ASMTransitResponse: transitResp.ASMTransitResponse,
		}, nil
	}

	if err := applyTransitionToMutableState(
		asmContext.GetKey(),
		*transition,
		mutableState,
	); err != nil {
		return TransitResponse{}, err
	}

	mutableStateTransition, err := mutableState.CloseTransition()
	if err != nil {
		return TransitResponse{}, err
	}

	_, err = shardContext.UpsertASM(
		ctx,
		&persistence.UpsertASMRequest{
			ShardID: shardContext.GetShardID(),
			ASMTransitions: []persistence.ASMTransition{
				mutableStateTransition,
			},
			// TODO: asm
			// CurrentASMTransition: ,
		},
	)
	if err != nil {
		return TransitResponse{}, err
	}

	newToken, err := mutableState.Token()
	if err != nil {
		return TransitResponse{}, err
	}

	return TransitResponse{
		ASMToken:           newToken,
		ASMTransitResponse: transitResp.ASMTransitResponse,
	}, nil
}

func applyTransitionToMutableState(
	asmKey definition.WorkflowKey,
	transition plugin.InstanceTransition,
	mutableState MutableState,
) error {
	pluginSeriazlier := mutableState.Plugin().Serializer()

	newTasks := make([]tasks.Task, 0, len(transition.Tasks))
	for _, task := range transition.Tasks {
		taskBlob, err := pluginSeriazlier.SerializeTask(task)
		if err != nil {
			return err
		}

		newTasks = append(newTasks, &tasks.ASMTask{
			WorkflowKey:         asmKey,
			Category:            task.Category(),
			VisibilityTimestamp: task.ScheduledTime(),
			// taskID and token will be assigned later by mutableState and shardContext
			Body: taskBlob,
		})
	}

	newEvents := make([]*historypb.HistoryEvent, 0, len(transition.HistoryEvents))
	for _, event := range transition.HistoryEvents {
		eventBlob, err := pluginSeriazlier.SerializeHistoryEvent(event)
		if err != nil {
			return err
		}

		newEvents = append(newEvents, &historypb.HistoryEvent{
			EventId:   event.EventID(),
			EventTime: timestamppb.New(event.EventTime()),
			// EventType: event.EventType(), not used
			// Version: assigned later by mutableState
			// TaskId: assigned later by mutableState
			Attributes: &historypb.HistoryEvent_Body{
				Body: eventBlob,
			},
		})
	}

	if transition.UpsertVisibility {
		return serviceerror.NewUnimplemented("UpsertVisibility is not supported yet")
	}

	mutableState.Transit(
		transition.Instance,
		newTasks,
		newEvents,
	)

	return nil
}
