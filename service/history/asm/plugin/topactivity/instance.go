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

package topactivity

import (
	"context"
	"encoding/json"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/topactivity/v1"
	"go.temporal.io/server/service/history/asm/plugin"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	instanceImpl struct {
		*topactivity.State
	}
)

type (
	TaskToken struct {
		ASMToken []byte
		Attempt  int32
	}
)

const (
	backoffDuration = 5 * time.Second
	maxAttempts     = 3

	maximumPageSize = 1000
)

func (t *TaskToken) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TaskToken) Deserialize(bytes []byte) error {
	return json.Unmarshal(bytes, t)
}

func (i *instanceImpl) Transit(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
) (plugin.TransitInstanceResponse, error) {
	switch request.Event.EventType() {
	case plugin.EventTypeTransit:
		return i.handleAPITransit(ctx, request, request.Event.(*plugin.TransitEvent))
	case plugin.EventTypeExecuteTask:
		return i.handleTask(ctx, request, request.Event.(*plugin.ExecuteTaskEvent))
	}

	return plugin.TransitInstanceResponse{}, nil
}

func (i *instanceImpl) StateSnapshot() plugin.StateSnapshot {
	return plugin.StateSnapshot{
		Core: i.State,
	}
}

func (i *instanceImpl) handleAPITransit(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.TransitEvent,
) (plugin.TransitInstanceResponse, error) {
	switch event.Request.(type) {
	case *DescribeRequest:
		return i.handleDescribe(ctx)
	case *GetTaskRequest:
		return i.handleGetTask(ctx, request, event)
	case *RespondCompletedRequest:
		return i.handleRespondCompleted(ctx, request, event)
	case *RespondFailedRequest:
		return i.handleRespondFailed(ctx, request, event)
	case *GetHistoryRequest:
		return i.handleGetHistory(ctx, request)
	default:
		return plugin.TransitInstanceResponse{}, serviceerror.NewInvalidArgument("unknown request type")
	}
}

func (i *instanceImpl) handleDescribe(
	ctx context.Context,
) (plugin.TransitInstanceResponse, error) {
	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: nil,
		NewInstanceStartRequest:   nil,
		ASMTransitResponse: &DescribeResponse{
			DescResponse: &workflowservice.DescribeTopActivityResponse{
				ActivityType:        i.ActivityType,
				TaskQueue:           i.TaskQueue,
				Attempt:             i.Attempt,
				StartedIdentity:     i.StartedIdentity,
				RequestId:           i.RequestId,
				AsmId:               i.AsmId,
				RunId:               i.RunId,
				StartedTime:         i.StartedTime,
				ScheduledTime:       i.ScheduledTime,
				ClosedTime:          i.ClosedTime,
				State:               i.State.State,
				StartToCloseTimeout: i.StartToCloseTimeout,
			},
		},
	}, nil
}

func (i *instanceImpl) handleGetTask(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.TransitEvent,
) (plugin.TransitInstanceResponse, error) {
	if i.State.ScheduledTime == nil || i.State.StartedTime != nil {
		return plugin.TransitInstanceResponse{},
			serviceerror.NewNotFound("activity task is not scheduled or already started")
	}

	now := event.EventTime()
	i.State.StartedTime = timestamppb.New(now)
	i.State.StartedIdentity = event.Request.(*historyservice.GetTopActivityTaskRequest).GetTaskRequest.GetIdentity()
	i.State.State = "Started"

	resp, err := request.Resources.HistoryEventLoader.Load(
		ctx,
		plugin.LoadHistoryEventRequest{
			HistoryEventToken: i.CreatedEventToken,
		},
	)
	if err != nil {
		return plugin.TransitInstanceResponse{}, err
	}

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: &plugin.InstanceTransition{
			Instance: i,
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   request.Resources.Metadata.HistoryEventCount + 1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityStartedEventAttributes{
							TopActivityStartedEventAttributes: &history.TopActivityStartedEventAttributes{
								Identity: i.State.StartedIdentity,
							},
						},
					},
				},
			},
			Tasks: []plugin.Task{
				newTask(
					activityTimerCategory,
					now.Add(i.StartToCloseTimeout.AsDuration()),
					&topactivity.TopActivityTask{
						Attributes: &topactivity.TopActivityTask_TopActivityTimeoutTaskAttributes{
							TopActivityTimeoutTaskAttributes: &topactivity.TopActivityTimeoutTaskAttributes{
								Attempt: i.Attempt,
							},
						},
					},
				),
			},
			UpsertVisibility: false,
		},

		NewInstanceStartRequest: nil,
		ASMTransitResponse: &GetTaskResponse{
			GetTaskResponse: &workflowservice.GetTopActivityTaskResponse{
				// TaskToken assigned in handler
				Input:               resp.HistoryEvent.(*Event).GetTopActivityCreatedEventAttributes().Input,
				Attempt:             i.Attempt,
				StartToCloseTimeout: i.StartToCloseTimeout,
			},
		},
	}, nil
}

func (i *instanceImpl) handleRespondCompleted(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.TransitEvent,
) (plugin.TransitInstanceResponse, error) {
	completedRequest := event.Request.(*historyservice.RespondTopActivityCompletedRequest)
	var taskToken TaskToken
	if err := taskToken.Deserialize(completedRequest.CompletionRequest.Token); err != nil {
		return plugin.TransitInstanceResponse{}, err
	}

	if taskToken.Attempt != i.Attempt {
		return plugin.TransitInstanceResponse{},
			serviceerror.NewNotFound("activity task not found")
	}

	now := event.EventTime()
	i.ScheduledTime = nil
	i.StartedTime = nil
	i.ClosedTime = timestamppb.New(now)
	i.State.State = "Completed"

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: &plugin.InstanceTransition{
			Instance: i,
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   request.Resources.Metadata.HistoryEventCount + 1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityCompletedEventAttributes{
							TopActivityCompletedEventAttributes: &history.TopActivityCompletedEventAttributes{
								Result:   completedRequest.CompletionRequest.Result,
								Identity: i.StartedIdentity,
							},
						},
					},
				},
			},
			Tasks:            nil,
			UpsertVisibility: false,
		},

		NewInstanceStartRequest: nil,
		ASMTransitResponse: &RespondCompletedResponse{
			CompletionResponse: &workflowservice.RespondTopActivityCompletedResponse{},
		},
	}, nil
}

func (i *instanceImpl) handleRespondFailed(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.TransitEvent,
) (plugin.TransitInstanceResponse, error) {
	failedRequest := event.Request.(*RespondFailedRequest)
	var taskToken TaskToken
	if err := taskToken.Deserialize(failedRequest.FailedRequest.Token); err != nil {
		return plugin.TransitInstanceResponse{}, err
	}

	if taskToken.Attempt != i.Attempt {
		return plugin.TransitInstanceResponse{},
			serviceerror.NewNotFound("activity task not found")
	}

	now := event.EventTime()
	i.ScheduledTime = nil
	i.StartedTime = nil
	i.ClosedTime = timestamppb.New(now)

	var tasks []plugin.Task
	if i.Attempt < maxAttempts {
		i.State.State = "Backoff"
		tasks = []plugin.Task{
			newTask(
				activityTimerCategory,
				now.Add(backoffDuration),
				&topactivity.TopActivityTask{
					Attributes: &topactivity.TopActivityTask_TopActivityRetryTaskAttributes{
						TopActivityRetryTaskAttributes: &topactivity.TopActivityRetryTaskAttributes{
							Attempt: i.Attempt,
						},
					},
				},
			),
		}
	} else {
		i.State.State = "Failed"
	}

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: &plugin.InstanceTransition{
			Instance: i,
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   request.Resources.Metadata.HistoryEventCount + 1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityFailedEventAttributes{
							TopActivityFailedEventAttributes: &history.TopActivityFailedEventAttributes{
								Failure: failedRequest.FailedRequest.Failure,
							},
						},
					},
				},
			},
			Tasks:            tasks,
			UpsertVisibility: false,
		},

		NewInstanceStartRequest: nil,
		ASMTransitResponse: &RespondFailedResponse{
			FailedResponse: &workflowservice.RespondTopActivityFailedResponse{},
		},
	}, nil
}

func (i *instanceImpl) handleGetHistory(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
) (plugin.TransitInstanceResponse, error) {

	iterator := request.Resources.HistoryEventsIterator

	var historyEvents []*history.TopActivityEvent

	pageSize := 0
	for iterator.HasNext() {
		historyEvent, err := iterator.Next()
		if err != nil {
			return plugin.TransitInstanceResponse{}, err
		}

		historyEvents = append(historyEvents, historyEvent.(*Event).TopActivityEvent)

		pageSize++
		if pageSize >= maximumPageSize {
			break
		}
	}

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: nil,
		NewInstanceStartRequest:   nil,
		ASMTransitResponse: &GetHistoryResponse{
			GetHistoryResponse: &workflowservice.GetTopActivityHistoryResponse{
				Events: historyEvents,
			},
		},
	}, nil
}

func (i *instanceImpl) handleTask(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.ExecuteTaskEvent,
) (plugin.TransitInstanceResponse, error) {
	task := event.Task.(*Task)
	switch attr := task.GetAttributes().(type) {
	case *topactivity.TopActivityTask_TopActivityTimeoutTaskAttributes:
		return i.handleTimeoutTask(ctx, request, event, attr)
	case *topactivity.TopActivityTask_TopActivityRetryTaskAttributes:
		return i.handleRetryTask(ctx, request, event, attr)
	default:
		return plugin.TransitInstanceResponse{}, serviceerror.NewInvalidArgument("unknown task type")
	}
}

func (i *instanceImpl) handleTimeoutTask(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.ExecuteTaskEvent,
	attr *topactivity.TopActivityTask_TopActivityTimeoutTaskAttributes,
) (plugin.TransitInstanceResponse, error) {
	if attr.TopActivityTimeoutTaskAttributes.Attempt != i.Attempt ||
		i.State.State != "Started" {
		return plugin.TransitInstanceResponse{},
			serviceerror.NewNotFound("activity task not found")
	}

	now := event.EventTime()
	i.ScheduledTime = nil
	i.StartedTime = nil
	i.ClosedTime = nil

	var tasks []plugin.Task
	if i.Attempt < maxAttempts {
		i.State.State = "Backoff"
		tasks = []plugin.Task{
			newTask(
				activityTimerCategory,
				now.Add(backoffDuration),
				&topactivity.TopActivityTask{
					Attributes: &topactivity.TopActivityTask_TopActivityRetryTaskAttributes{
						TopActivityRetryTaskAttributes: &topactivity.TopActivityRetryTaskAttributes{
							Attempt: i.Attempt,
						},
					},
				},
			),
		}
	} else {
		i.State.State = "TimedOut"
	}

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: &plugin.InstanceTransition{
			Instance: i,
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   request.Resources.Metadata.HistoryEventCount + 1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityTimedoutEventAttributes{
							TopActivityTimedoutEventAttributes: &history.TopActivityTimedOutEventAttributes{
								TimeoutType: enums.TIMEOUT_TYPE_START_TO_CLOSE,
							},
						},
					},
				},
			},
			Tasks:            tasks,
			UpsertVisibility: false,
		},

		NewInstanceStartRequest: nil,
		ASMTransitResponse:      nil,
	}, nil
}

func (i *instanceImpl) handleRetryTask(
	ctx context.Context,
	request plugin.TransitInstanceRequest,
	event *plugin.ExecuteTaskEvent,
	attr *topactivity.TopActivityTask_TopActivityRetryTaskAttributes,
) (plugin.TransitInstanceResponse, error) {
	if attr.TopActivityRetryTaskAttributes.Attempt != i.Attempt ||
		i.State.State != "Backoff" {
		return plugin.TransitInstanceResponse{},
			serviceerror.NewNotFound("activity task not found")
	}

	now := event.EventTime()
	i.ScheduledTime = timestamppb.New(now)
	i.Attempt++
	i.StartedTime = nil
	i.ClosedTime = nil
	i.State.State = "Scheduled"

	return plugin.TransitInstanceResponse{
		CurrentInstanceTransition: &plugin.InstanceTransition{
			Instance: i,
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   request.Resources.Metadata.HistoryEventCount + 1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityScheduledEventAttributes{
							TopActivityScheduledEventAttributes: &history.TopActivityScheduledEventAttributes{
								Attempt: int64(i.Attempt),
							},
						},
					},
				},
			},
			Tasks:            nil,
			UpsertVisibility: false,
		},

		NewInstanceStartRequest: nil,
		ASMTransitResponse:      nil,
	}, nil
}
