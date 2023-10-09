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
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	taskExecutor struct {
		shard    shard.Context
		asmCache Cache
	}
)

func NewTaskExecutor(
	shard shard.Context,
	asmCache Cache,
) queues.Executor {
	return &taskExecutor{
		shard:    shard,
		asmCache: asmCache,
	}
}

func (t *taskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) (resp queues.ExecuteResponse) {
	task := executable.GetTask()
	taskType := task.GetType().String() // TODO: asm Type() is not init for asm tasks
	namespaceTag, _ := getNamespaceTagAndReplicationStateByID(
		t.shard.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	// if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
	// 	// TODO: exclude task types here if we believe it's safe & necessary to execute
	// 	// them during namespace handover.
	// 	// TODO: move this logic to queues.Executable when metrics tag doesn't need to
	// 	// be returned from task executor
	// 	return queues.ExecuteResponse{
	// 		ExecutionMetricTags: metricsTags,
	// 		ExecutedAsActive:    true,
	// 		ExecutionErr:        consts.ErrNamespaceHandover,
	// 	}
	// }

	asmTask, ok := task.(*tasks.ASMTask)
	if !ok {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        serviceerror.NewInternal("Unknown asm task"),
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	asmContext, releaseFn, err := t.asmCache.GetOrCreateWorkflowExecution(
		ctx,
		t.shard,
		asmTask.WorkflowKey,
		LockPriorityLow,
	)
	if common.IsContextDeadlineExceededErr(err) {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        consts.ErrResourceExhaustedBusyWorkflow,
		}
	}
	defer func() { releaseFn(resp.ExecutionErr) }()

	mutableState, err := asmContext.LoadMutableState(ctx, t.shard)
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	if err := mutableState.ValidateToken(asmTask.ASMToken); err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	decodedTask, err := mutableState.Plugin().Serializer().DeserializeTask(asmTask.Category, asmTask.Body)
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	// Following are basically the same as the logic in engineImpl.Transit

	currentToken, err := mutableState.Token()
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	transitResp, err := mutableState.PluginInstance().Transit(
		ctx,
		plugin.TransitInstanceRequest{
			RequestASMToken: asmTask.ASMToken,
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
					t.shard.GetShardID(),
					currentBranchToken,
					t.shard.GetExecutionManager(),
					mutableState.Plugin().Serializer(),
				),
				HistoryEventLoader: newHistoryEventLoader(
					t.shard.GetShardID(),
					t.shard.GetEventsCache(),
					asmContext.GetKey(),
					currentBranchToken,
					mutableState.Plugin().Serializer(),
				),
				Logger:         t.shard.GetLogger(),
				MetricsHandler: t.shard.GetMetricsHandler(),
			},
			NewInstanceKey: definition.NewWorkflowKey(
				asmTask.NamespaceID,
				asmTask.WorkflowID,
				uuid.New().String(),
			),
			Event: plugin.NewExecuteTaskEvent(
				asmTask.GetVisibilityTime(),
				decodedTask,
			),
		},
	)
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	if transitResp.NewInstanceStartRequest != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        serviceerror.NewUnimplemented("NewInstanceStartRequest is not supported yet"),
		}
	}

	transition := transitResp.CurrentInstanceTransition
	if transition == nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        nil,
		}
	}

	if err := applyTransitionToMutableState(
		asmContext.GetKey(),
		*transition,
		mutableState,
	); err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	mutableStateTransition, err := mutableState.CloseTransition()
	if err != nil {
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutionErr:        err,
		}
	}

	_, err = t.shard.UpsertASM(
		ctx,
		&persistence.UpsertASMRequest{
			ShardID: t.shard.GetShardID(),
			ASMTransitions: []persistence.ASMTransition{
				mutableStateTransition,
			},
			// TODO: asm
			// CurrentASMTransition: ,
		},
	)
	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutionErr:        err,
	}
}

func getNamespaceTagAndReplicationStateByID(
	registry namespace.Registry,
	namespaceID string,
) (metrics.Tag, enumspb.ReplicationState) {
	namespace, err := registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return metrics.NamespaceUnknownTag(), enumspb.REPLICATION_STATE_UNSPECIFIED
	}

	return metrics.NamespaceTag(namespace.Name().String()), namespace.ReplicationState()
}
