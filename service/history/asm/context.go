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
	"fmt"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	LockPriority int

	Context interface {
		GetKey() definition.WorkflowKey

		Lock(ctx context.Context, lockPriority LockPriority) error
		Unlock(lockPriority LockPriority)

		IsDirty() bool
		Clear()

		LoadMutableState(ctx context.Context, shardContext shard.Context) (MutableState, error)
	}

	contextImpl struct {
		key             definition.WorkflowKey
		logger          log.Logger
		throttledLogger log.ThrottledLogger
		metricsHandler  metrics.Handler
		config          *configs.Config
		plulginRegistry Registry

		mutex        locks.PriorityMutex
		mutableState MutableState
	}
)

const (
	LockPriorityHigh LockPriority = 0
	LockPriorityLow  LockPriority = 1
)

func NewContext(
	config *configs.Config,
	key definition.WorkflowKey,
	plulginRegistry Registry,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
) *contextImpl {
	return &contextImpl{
		key:             key,
		logger:          logger,
		throttledLogger: throttledLogger,
		metricsHandler:  metricsHandler.WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		config:          config,
		mutex:           locks.NewPriorityMutex(),
		plulginRegistry: plulginRegistry,
	}
}

func (c *contextImpl) GetKey() definition.WorkflowKey {
	return c.key
}

func (c *contextImpl) Lock(ctx context.Context, lockPriority LockPriority) error {
	switch lockPriority {
	case LockPriorityHigh:
		return c.mutex.LockHigh(ctx)
	case LockPriorityLow:
		return c.mutex.LockLow(ctx)
	default:
		panic(fmt.Sprintf("unknown lock priority: %v", lockPriority))
	}
}

func (c *contextImpl) Unlock(lockPriority LockPriority) {
	switch lockPriority {
	case LockPriorityHigh:
		c.mutex.UnlockHigh()
	case LockPriorityLow:
		c.mutex.UnlockLow()
	default:
		panic(fmt.Sprintf("unknown lock priority: %v", lockPriority))
	}
}

func (c *contextImpl) IsDirty() bool {
	if c.mutableState == nil {
		return false
	}
	return c.mutableState.IsDirty()
}

func (c *contextImpl) Clear() {
	c.mutableState = nil
}

func (c *contextImpl) LoadMutableState(
	ctx context.Context,
	shardContext shard.Context,
) (MutableState, error) {

	if c.mutableState == nil {
		var err error
		c.mutableState, err = c.getMutableState(ctx, shardContext)
		if err != nil {
			return nil, err
		}
	}

	// TODO: should be done by the engine?
	// not here
	if err := c.mutableState.StartTransition(); err != nil {
		return nil, err
	}

	return c.mutableState, nil
}

func (c *contextImpl) getMutableState(
	ctx context.Context,
	shardContext shard.Context,
) (MutableState, error) {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.key.NamespaceID),
	)
	if err != nil {
		return nil, err
	}

	resp, err := shardContext.GetASM(ctx, &persistence.GetASMRequest{
		ShardID: shardContext.GetShardID(),
		ASMKey:  c.key,
	})
	if err != nil {
		return nil, err
	}

	return NewMutableStateFromDB(
		shardContext,
		c.plulginRegistry,
		namespaceEntry,
		resp.Execution,
		resp.ExecutionMetadata,
		resp.DBRecordVersion,
	)
}
