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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/topactivity/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/tasks"
)

var serializer = &serializerImpl{}

type (
	serializerImpl struct {
	}
)

func (s *serializerImpl) SerializeState(state any) (*commonpb.DataBlob, error) {
	typedState := state.(*topactivity.State)
	return serialization.ProtoEncodeBlob(typedState, enums.ENCODING_TYPE_PROTO3)
}

func (s *serializerImpl) DeserializeState(blob *commonpb.DataBlob) (any, error) {
	typedState := &topactivity.State{}
	return typedState, serialization.ProtoDecodeBlob(blob, typedState)
}

func (s *serializerImpl) SerializeHistoryEvent(event plugin.HistoryEvent) (*commonpb.DataBlob, error) {
	typedEvent := event.(*Event)
	return serialization.ProtoEncodeBlob(typedEvent.TopActivityEvent, enums.ENCODING_TYPE_PROTO3)
}

func (s *serializerImpl) DeserializeHistoryEvent(blob *commonpb.DataBlob) (plugin.HistoryEvent, error) {
	event := &history.TopActivityEvent{}
	return &Event{
		TopActivityEvent: event,
	}, serialization.ProtoDecodeBlob(blob, event)
}

func (s *serializerImpl) SerializeTask(task plugin.Task) (*commonpb.DataBlob, error) {
	typedTask := task.(*Task)
	return serialization.ProtoEncodeBlob(typedTask.TopActivityTask, enums.ENCODING_TYPE_PROTO3)
}

func (s *serializerImpl) DeserializeTask(category tasks.Category, blob *commonpb.DataBlob) (plugin.Task, error) {
	task := &topactivity.TopActivityTask{}
	return newTask(
		category,
		time.Now(), // TODO,
		task,
	), serialization.ProtoDecodeBlob(blob, task)
}
