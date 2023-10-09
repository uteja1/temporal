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
	"time"

	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/topactivity/v1"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var Type = "topactivity"

type (
	TopActivity struct{}

	StartRequest  = historyservice.CreateTopActivityRequest
	StartResponse = historyservice.CreateTopActivityResponse

	DescribeRequest  = historyservice.DescribeTopActivityRequest
	DescribeResponse = historyservice.DescribeTopActivityResponse

	GetTaskRequest  = historyservice.GetTopActivityTaskRequest
	GetTaskResponse = historyservice.GetTopActivityTaskResponse

	RespondCompletedRequest  = historyservice.RespondTopActivityCompletedRequest
	RespondCompletedResponse = historyservice.RespondTopActivityCompletedResponse

	RespondFailedRequest  = historyservice.RespondTopActivityFailedRequest
	RespondFailedResponse = historyservice.RespondTopActivityFailedResponse

	GetHistoryRequest  = historyservice.GetTopActivityHistoryRequest
	GetHistoryResponse = historyservice.GetTopActivityHistoryResponse
)

func New() plugin.Plugin {
	return &TopActivity{}
}

func (t *TopActivity) Type() string {
	return Type
}

func (t *TopActivity) Configs() plugin.Configs {
	return plugin.Configs{}
}

func (t *TopActivity) TaskCategories() []tasks.Category {
	return categories
}

func (t *TopActivity) Serializer() plugin.Serializer {
	return serializer
}

func (t *TopActivity) Start(
	ctx context.Context,
	request plugin.StartInstanceRequest,
) (plugin.StartInstanceResponse, error) {
	startRequest := request.ASMStartRequest.(*StartRequest)
	now := time.Now()
	createEventToken := request.Resources.HistoryEventTokenGenerator.Generate(1)
	return plugin.StartInstanceResponse{
		NewInstanceTransition: plugin.InstanceTransition{
			Instance: &instanceImpl{
				State: &topactivity.State{
					ActivityType:        startRequest.StartRequest.ActivityType,
					TaskQueue:           startRequest.StartRequest.TaskQueue,
					Attempt:             1,
					StartedIdentity:     "",
					RequestId:           startRequest.StartRequest.RequestId,
					AsmId:               startRequest.StartRequest.Id,
					RunId:               request.Key.RunID,
					StartedTime:         nil,
					ScheduledTime:       timestamppb.New(now),
					ClosedTime:          nil,
					State:               "Scheduled",
					StartToCloseTimeout: startRequest.StartRequest.StartToCloseTimeout,
					CreatedEventToken:   createEventToken,
				},
			},
			HistoryEvents: []plugin.HistoryEvent{
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   1,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityCreatedEventAttributes{
							TopActivityCreatedEventAttributes: &history.TopActivityCreatedEventAttributes{
								ActivityType:        startRequest.StartRequest.ActivityType,
								TaskQueue:           startRequest.StartRequest.TaskQueue,
								Input:               startRequest.StartRequest.Input,
								RequestId:           startRequest.StartRequest.RequestId,
								AsmId:               startRequest.StartRequest.Id,
								RunId:               request.Key.RunID,
								StartToCloseTimeout: startRequest.StartRequest.StartToCloseTimeout,
								Identity:            startRequest.StartRequest.Identity,
							},
						},
					},
				},
				&Event{
					TopActivityEvent: &history.TopActivityEvent{
						EventId:   2,
						EventTime: timestamppb.New(now),
						Attributes: &history.TopActivityEvent_TopActivityScheduledEventAttributes{
							TopActivityScheduledEventAttributes: &history.TopActivityScheduledEventAttributes{
								Attempt: 1,
							},
						},
					},
				},
			},
			Tasks:            nil,
			UpsertVisibility: false,
		},
		ASMStartResponse: &StartResponse{
			StartResponse: &workflowservice.CreateTopActivityResponse{
				RunId: request.Key.RunID,
			},
		},
	}, nil
}

func (t *TopActivity) Restore(
	ctx context.Context,
	request plugin.RestoreInstanceRequest,
) (plugin.RestoreInstanceResponse, error) {
	return plugin.RestoreInstanceResponse{
		Instance: &instanceImpl{
			State: request.StateSnapshot.Core.(*topactivity.State),
		},
	}, nil
}
