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

package history

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/asm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	asmQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
		asm.Cache
	}

	asmQueueFactory struct {
		asmQueueFactoryParams
		QueueFactoryBase

		category tasks.Category
	}
)

func NewASMQueueFactory(
	category tasks.Category,
	params asmQueueFactoryParams,
) QueueFactory {
	return &asmQueueFactory{
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					// TODO: asm reusing config from transfer queue for now
					WorkerCount:             params.Config.TransferProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:  params.Config.TransferProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights: params.Config.TransferProcessorSchedulerStandbyRoundRobinWeights,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TransferProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					transferQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.TransferQueueMaxReaderCount()),
			),
		},
		category:              category,
		asmQueueFactoryParams: params,
	}

}

func (f *asmQueueFactory) CreateQueue(
	shard shard.Context,
	_ wcache.Cache,
) queues.Queue {
	// TODO: asm reusing config from transfer queue for now
	logger := log.With(shard.GetLogger(), tag.NewStringTag("component", f.category.Name()+"queue-processor"))
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope))

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			f.ClusterMetadata.GetCurrentClusterName(),
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	executor := asm.NewTaskExecutor(
		shard,
		f.Cache,
	)
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	executableFactory := queues.NewExecutableFactory(
		executor,
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shard.GetTimeSource(),
		shard.GetNamespaceRegistry(),
		shard.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
	)

	queueOptions := &queues.Options{
		ReaderOptions: queues.ReaderOptions{
			BatchSize:            f.Config.TransferTaskBatchSize,
			MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
			PollBackoffInterval:  f.Config.TransferProcessorPollBackoffInterval,
		},
		MonitorOptions: queues.MonitorOptions{
			PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
			ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
			SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
		},
		MaxPollRPS:                          f.Config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                     f.Config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:    f.Config.TransferProcessorMaxPollIntervalJitterCoefficient,
		CheckpointInterval:                  f.Config.TransferProcessorUpdateAckInterval,
		CheckpointIntervalJitterCoefficient: f.Config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxReaderCount:                      f.Config.TransferQueueMaxReaderCount,
	}

	if f.category.Type() == tasks.CategoryTypeImmediate {
		return queues.NewImmediateQueue(
			shard,
			f.category,
			shardScheduler,
			rescheduler,
			queueOptions,
			f.HostReaderRateLimiter,
			queues.GrouperNamespaceID{},
			logger,
			metricsHandler,
			executableFactory,
		)
	} else {
		return queues.NewScheduledQueue(
			shard,
			f.category,
			shardScheduler,
			rescheduler,
			executableFactory,
			queueOptions,
			f.HostReaderRateLimiter,
			logger,
			metricsHandler,
		)
	}

}
