package chasm

import (
	"time"

	"go.temporal.io/api/common/v1"
)

type (
	Event interface {
		EventType() EventType
		EventTime() time.Time
	}

	EventType int

	ASMEvent struct {
		Event any
	}

	ApplyHistoryEvent struct {
		HistoryEvent HistoryEvent
	}

	SyncSnapshotEvent struct {
		State StateSnapshot
	}

	SyncMutationEvent struct {
		StateMutation StateMutation
	}

	TerminateEvent struct {
		Reason   string
		Details  common.Payload
		Identity string
	}
)

const (
	EventTypeASM EventType = iota
	EventTypeApplyHistory
	EventTypeSyncSnapshot // replication only
	EventTypeSyncMutation // replication only
	EventTypeTerminate
)
