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

package tasks

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

type (
	StateTransitionItem struct {
		ID      int64
		Version int64
	}

	ASMTask struct {
		definition.WorkflowKey
		Category            Category
		VisibilityTimestamp time.Time
		TaskID              int64
		ASMToken            []byte

		Body *commonpb.DataBlob
	}
)

func (a *ASMTask) GetKey() Key {
	if a.Category.Type() == CategoryTypeImmediate {
		return NewImmediateKey(a.TaskID)
	}
	return NewKey(a.VisibilityTimestamp, a.TaskID)
}

func (a *ASMTask) GetVersion() int64 {
	panic("GetVersion should not be called for ASMTask")
}

func (a *ASMTask) SetVersion(version int64) {
	panic("SetVersion should not be called for ASMTask")
}

func (a *ASMTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ASMTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ASMTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ASMTask) SetVisibilityTime(t time.Time) {
	a.VisibilityTimestamp = t
}

func (a *ASMTask) GetCategory() Category {
	return a.Category
}

func (a *ASMTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_UNSPECIFIED
}
