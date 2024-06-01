// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

func taskWorkflowKey(task tasks.Task) definition.WorkflowKey {
	return definition.NewWorkflowKey(task.GetNamespaceID(), task.GetWorkflowID(), task.GetRunID())
}

func getWorkflowExecutionContextForTask(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	task tasks.Task,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	return getWorkflowExecutionContext(
		ctx,
		shardContext,
		workflowCache,
		taskWorkflowKey(task),
		workflow.LockPriorityLow,
	)
}

func getWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	key definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	if key.GetRunID() == "" {
		return getCurrentWorkflowExecutionContext(
			ctx,
			shardContext,
			workflowCache,
			key.NamespaceID,
			key.WorkflowID,
			lockPriority,
		)
	}

	namespaceID := namespace.ID(key.GetNamespaceID())
	execution := &commonpb.WorkflowExecution{
		WorkflowId: key.GetWorkflowID(),
		RunId:      key.GetRunID(),
	}
	// workflowCache will automatically use short context timeout when
	// locking workflow for all background calls, we don't need a separate context here
	weContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespaceID,
		execution,
		lockPriority,
	)
	if common.IsContextDeadlineExceededErr(err) {
		// TODO: make sure this doesn't count against our SLA if this happens while handling an API request.
		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	return weContext, release, err
}

func getCurrentWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	namespaceID string,
	workflowID string,
	lockPriority workflow.LockPriority,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	shardOwnershipAsserted := false
	currentRunID, err := wcache.GetCurrentRunID(
		ctx,
		shardContext,
		workflowCache,
		&shardOwnershipAsserted,
		namespaceID,
		workflowID,
		lockPriority,
	)
	if err != nil {
		return nil, nil, err
	}

	wfContext, release, err := getWorkflowExecutionContext(
		ctx,
		shardContext,
		workflowCache,
		definition.NewWorkflowKey(namespaceID, workflowID, currentRunID),
		lockPriority,
	)
	if err != nil {
		return nil, nil, err
	}

	mutableState, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		release(err)
		return nil, nil, err
	}

	if mutableState.IsWorkflowExecutionRunning() {
		return wfContext, release, nil
	}

	// for close workflow we need to check if it is still the current run
	// since it's possible that the workflowID has a newer run before it's locked

	currentRunID, err = wcache.GetCurrentRunID(
		ctx,
		shardContext,
		workflowCache,
		&shardOwnershipAsserted,
		namespaceID,
		workflowID,
		lockPriority,
	)
	if err != nil {
		// release with nil error to prevent mutable state from being unloaded from the cache
		release(nil)
		return nil, nil, err
	}

	if currentRunID != wfContext.GetWorkflowKey().RunID {
		// release with nil error to prevent mutable state from being unloaded from the cache
		release(nil)
		return nil, nil, consts.ErrLocateCurrentWorkflowExecution
	}

	return wfContext, release, nil
}

// stateMachineEnvironment provides basic functionality for state machine task execution and handling of API requests.
type stateMachineEnvironment struct {
	shardContext   shard.Context
	cache          wcache.Cache
	metricsHandler metrics.Handler
	logger         log.Logger
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Does **not** reload mutable state if validate reports it is stale. Not meant to be called directly, call
// [loadAndValidateMutableState] instead.
func (e *stateMachineEnvironment) loadAndValidateMutableStateNoReload(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState, bool) error,
	potentialStaleState bool,
) (workflow.MutableState, error) {
	mutableState, err := wfCtx.LoadMutableState(ctx, e.shardContext)
	if err != nil {
		return nil, err
	}

	return mutableState, validate(mutableState, potentialStaleState)
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Reloads mutable state and retries if validator returns a [queues.StaleStateError].
func (e *stateMachineEnvironment) loadAndValidateMutableState(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState, bool) error,
) (workflow.MutableState, error) {

	// TODO: for task processing code path, we know mutable state can't be stale
	// so there's no need to reload
	// we can tell if it's task processing call by checking the caller type in the context
	// or explicitly pass in a parameter

	mutableState, err := e.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate, true)
	if err == nil {
		return mutableState, nil
	}

	if !errors.Is(err, consts.ErrStaleState) {
		return nil, err
	}
	e.metricsHandler.Counter(metrics.StaleMutableStateCounter.Name()).Record(1)
	wfCtx.Clear()

	return e.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate, false)
}

// validateStateMachineRef compares the ref and associated state machine's version and transition count to detect staleness.
func (e *stateMachineEnvironment) validateStateMachineRef(
	ms workflow.MutableState,
	ref hsm.Ref,
	potentialStaleState bool,
) error {

	if ref.StateMachineRef.MutableStateTransitionCount == 0 {
		// transtion history was disabled when the ref is generated, fallback to the old validation logic
		return e.validateStateMachineRefByVersion(ms, ref, potentialStaleState)
	}

	err := workflow.TransitionHistoryStalenessCheck(
		ms.GetExecutionInfo().GetTransitionHistory(),
		ref.StateMachineRef.MutableStateNamespaceFailoverVersion,
		ref.StateMachineRef.MutableStateTransitionCount,
	)
	if err != nil {
		return err
	}

	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			return fmt.Errorf("%w: %w", consts.ErrStaleReference, err)
		}
		return fmt.Errorf("%w: %w", serviceerror.NewInternal("node lookup failed"), err)
	}

	// Only check for strict equality if the ref has non zero MachineTransitionCount, which marks the task as non-concurrent.
	if ref.StateMachineRef.MachineTransitionCount != 0 && node.TransitionCount() != ref.StateMachineRef.MachineTransitionCount {
		return fmt.Errorf("%w: state machine transitions != task transitions", consts.ErrStaleReference)
	}
	return nil
}

func (e *stateMachineEnvironment) validateStateMachineRefByVersion(
	ms workflow.MutableState,
	ref hsm.Ref,
	potentialStaleState bool,
) (retErr error) {

	// TODO: we need some refactoring here to take advantage of the fact that task processing logic will
	// never encounter a stale mutable state, i.e potentialStaleState will always be false. to avoid unnecessary
	// mutable state reloads.

	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			if potentialStaleState {
				return fmt.Errorf("%w: %w", consts.ErrStaleState, err)
			}
			return fmt.Errorf("%w: %w", consts.ErrStaleReference, err)
		}
		return fmt.Errorf("%w: %w", serviceerror.NewInternal("node lookup failed"), err)
	}

	if node.InitialNamespaceFailoverVersion() != ref.StateMachineRef.InitialNamespaceFailoverVersion {
		// NOTE: we can't really tell if mutable state is stale or not just by comparing node initial version.
		// Even if node contains a lower initial version, it may not be stale if the entire history branch is newer.
		// It's just this node was created earlier with a lower version.
		// We have to trust mutable state reload will give us the up-to-date state.
		if potentialStaleState {
			return fmt.Errorf("%w: state machine ref initial failover version mismatch", consts.ErrStaleState)
		}
		return fmt.Errorf("%w: state machine ref initial failover version mismatch", consts.ErrStaleReference)
	}

	// At this point, we have verified the ref is for the state machine node on the current branch.
	// But we can NOT tell if the state transtion that generated the ref is on the current branch or a different branch
	// since both current version and machine transition count on the node will be reset during ms rebuild.
	//
	// For task processing we can resolve this by recording the shard clock in the mutable state when it's rebuilt.
	// (search for all cases where mutableState.SetUpdateCondition() is called)
	// Then compare the shard clock with the taskID before processing the task. This invalidates all
	// tasks generated before the ms rebuild.
	// Then we can assume here the ref is alwasy for the current branch and verify current version
	// and transition count as usually.
	// i.e. for non-concurrent task, current version and transition count must match
	//      for concurrent task, since there's no stale state for task processing, no validation needed.
	//
	// For nexus completion API call, since nexus operation state can jump directly from scheduled
	// to completed, we can just invoke the handler logic. Stale state will automatically be detected
	// when persisting the state change. Stale ref will encounter invalid transition error and be dropped.
	// Even if there are multiple operation started events on different branches and the completion request is
	// for the started event on a different branch, we can still apply the completion to the current branch
	// since we have verified the ref is for the nexus operation node on the current branch.

	// TODO: implement ^ logic
	return nil
}

// getValidatedMutableState loads mutable state and validates it with the given function.
// validate must not mutate the state.
func (e *stateMachineEnvironment) getValidatedMutableState(
	ctx context.Context,
	key definition.WorkflowKey,
	validate func(workflow.MutableState, bool) error,
) (workflow.Context, wcache.ReleaseCacheFunc, workflow.MutableState, error) {
	// TODO: for api call, we should use a LockPriorityHigh
	// we can tell if it's frontend api call by checking the caller type in the context
	// or explicitly pass in a parameter
	wfCtx, release, err := getWorkflowExecutionContext(ctx, e.shardContext, e.cache, key, workflow.LockPriorityLow)
	if err != nil {
		return nil, nil, nil, err
	}

	ms, err := e.loadAndValidateMutableState(ctx, wfCtx, validate)

	if err != nil {
		// Release now with no error to prevent mutable state from being unloaded from the cache.
		release(nil)
		return nil, nil, nil, err
	}
	return wfCtx, release, ms, nil
}

func (e *stateMachineEnvironment) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) (retErr error) {
	wfCtx, release, ms, err := e.getValidatedMutableState(
		ctx, ref.WorkflowKey, func(ms workflow.MutableState, potentialStaleState bool) error {
			return e.validateStateMachineRef(ms, ref, potentialStaleState)
		},
	)
	if err != nil {
		return err
	}
	var accessed bool
	defer func() {
		if accessType == hsm.AccessWrite && accessed {
			release(retErr)
		} else {
			release(nil)
		}
	}()
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		return err
	}
	accessed = true
	if err := accessor(node); err != nil {
		return err
	}
	if accessType == hsm.AccessRead {
		return nil
	}

	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// Can't use UpdateWorkflowExecutionAsActive since it updates the current run, and we are operating on closed
		// workflows.
		return wfCtx.SubmitClosedWorkflowSnapshot(ctx, e.shardContext, workflow.TransactionPolicyActive)
	}
	return wfCtx.UpdateWorkflowExecutionAsActive(ctx, e.shardContext)
}

func (e *stateMachineEnvironment) Now() time.Time {
	return e.shardContext.GetTimeSource().Now()
}
