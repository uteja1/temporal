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

	"go.temporal.io/server/api/topactivity/v1"
	"go.temporal.io/server/service/history/tasks"
)

var (
	activityTransferCategory = tasks.NewCategory(
		10,
		tasks.CategoryTypeImmediate,
		"activity-transfer",
	)

	activityTimerCategory = tasks.NewCategory(
		11,
		tasks.CategoryTypeScheduled,
		"activity-timer",
	)

	categories = []tasks.Category{
		activityTransferCategory,
		activityTimerCategory,
	}
)

type (
	Task struct {
		category      tasks.Category
		scheduledTime time.Time

		*topactivity.TopActivityTask
	}
)

func newTask(
	category tasks.Category,
	scheduledTime time.Time,
	task *topactivity.TopActivityTask,
) *Task {
	return &Task{
		category:      category,
		scheduledTime: scheduledTime,

		TopActivityTask: task,
	}
}

func (t *Task) Category() tasks.Category {
	return t.category
}

func (t *Task) TaskType() string {
	// TODO: not used
	return ""
}

func (t *Task) ScheduledTime() time.Time {
	return t.scheduledTime
}
