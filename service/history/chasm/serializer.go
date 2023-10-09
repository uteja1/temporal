package chasm

import (
	commonpb "go.temporal.io/api/common/v1"
)

type (
	Serializer interface {
		SerializeCoreState(any) (commonpb.DataBlob, error)
		DeserializeCoreState(blob commonpb.DataBlob) (any, error)

		SerializeSubState(subStateType string, subState any) (commonpb.DataBlob, error)
		DeserializeSubState(subStateType string, blob commonpb.DataBlob) (any, error)

		SerializeHistoryEvent(HistoryEvent) (commonpb.DataBlob, error)
		DeserializeHistoryEvent(blob commonpb.DataBlob) (HistoryEvent, error)

		SerializeTask(Task) (commonpb.DataBlob, error)
		DeserializeTask(category TaskCategory, blob commonpb.DataBlob) (Task, error)
	}
)
