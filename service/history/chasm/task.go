package chasm

import (
	"context"
	"time"
)

type (
	Task interface {
		Category() TaskCategory
		TaskType() string
		ScheduledTime() time.Time
	}

	TaskCategory struct {
		Type TaskCategoryType
		Name string
		ID   int64 // we probably don't need this as long as Name is unique
	}

	TaskCategoryType int
)

const (
	TaskCategoryTypeImmediate TaskCategoryType = iota
	TaskCategoryTypeScheduled
)

type (
	TaskExecutor interface {
		Execute(context.Context, ExecuteTaskRequest) (ExecuteTaskResponse, error)
	}

	ExecuteTaskRequest struct {
		ASMContext ASMContext
		Key        ASMKey

		Task Task
	}

	ExecuteTaskResponse struct {
		// extra metric tags?
	}
)

type (
	TaskVerifier interface {
		Verify(context.Context, VerifyTaskRequest) (VerifyTaskResponse, error)
	}

	VerifyTaskRequest = ExecuteTaskRequest

	VerifyTaskResponse struct {
		Verified bool
	}
)
