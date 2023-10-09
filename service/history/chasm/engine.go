package chasm

import "context"

type (
	// Engine is the interface of the framework that executes ASM.
	// This is not today's history engine which bind to the life cycle of a history shard.
	// It's more like today's history handler.
	Engine interface {
		Start(context.Context, StartRequest) (StartResponse, error)
		TransitWithStart(context.Context, TransitWithStartRequest) (TransitWithStartResponse, error)
		Transit(context.Context, TransitRequest) (TransitResponse, error)
		Poll(context.Context, PollRequest) (PollResponse, error)
		Reset(context.Context, ResetRequest) (ResetResponse, error)
		Terminate(context.Context, TerminateRequest) (TerminateResponse, error)
		Delete(context.Context, DeleteRequest) (DeleteResponse, error)

		// Following two only needed for admin operations.
		// so it should live in a separate interface.
		// Engine def is for only for ASM implementation.
		// Import(context.Context, ImportRequest) (ImportResponse, error)
		// Rebuild(context.Context, RebuildRequest) (RebuildResponse, error)
	}
)

type (
	StartRequest struct {
		// Type of the ASM to start.
		ASMType string

		NamespaceID string

		ID            string
		IDReusePolicy IDReusePolicy

		RequestID string

		ASMStartRequest any
	}

	StartResponse struct {
		ASMContext ASMContext

		ASMStartResponse any
	}

	TransitWithStartRequest struct {
		ASMType string

		NamespaceID string

		ID            string
		IDReusePolicy IDReusePolicy

		RequestID string

		ASMStartRequest   any
		ASMTransitRequest any
	}

	TransitWithStartResponse struct {
		ASMContext ASMContext

		ASMStartResponse   any
		ASMTransitResponse any
	}

	TransitRequest struct {
		ASMContext ASMContext
		ASMRef     ASMRef

		ASMTransitRequest any
	}

	TransitResponse struct {
		ASMContext ASMContext

		ASMTransitResponse any
	}

	PollRequest struct {
		ASMContext ASMContext
		ASMRef     ASMRef

		ASMCondition  ASMCondition
		ASMStateQuery ASMStateQuery
		PollToken     []byte

		ASMPollRequest any
	}

	PollResponse struct {
		ASMContext ASMContext
		PollToken  []byte

		ASMPollResponse any
	}

	ResetRequest struct {
		ASMContext ASMContext
		ASMRef     ASMRef

		RequestID                    string
		HistoryEventReapplyPredicate HistoryEventReapplyPredicate
	}

	HistoryEventReapplyPredicate func(event HistoryEvent) bool

	ResetResponse struct {
		ASMContext  ASMContext
		ResetASMKey ASMKey
	}

	TerminateRequest struct {
		// TODO: do we need this for terminate?
		ASMContext ASMContext
		ASMRef     ASMRef

		ASMTerminateRequest any
	}

	TerminateResponse struct {
		ASMContext ASMContext

		ASMTerminateResponse any
	}

	DeleteRequest struct {
		// TODO: do we need this for delete?
		ASMContext ASMContext
		ASMRef     ASMRef
	}

	DeleteResponse struct{}
)

type (
	ASMKey struct {
		NamespaceID string
		ID          string
		RunID       string
	}

	ASMRef struct {
		NamespaceID string
		ID          string

		// At most one of the following fields can be set.
		// Optional
		RunID string
		// Optional
		ChainFirstRunID string
	}

	ASMContext []byte

	IDReusePolicy int

	ASMCondition struct {
		ASMEventCondition ASMEventCondition
		ASMStateCondition ASMStateCondition
	}

	ASMEventCondition struct {
		TargetEventID int64
	}

	ASMStateCondition struct {
		StatePredicate func(ASMState any) bool
	}

	ASMStateQuery func(ASMMetadata ASMMetadata, ASMState any) any

	ASMMetadata struct {
		ASMKey      ASMKey
		LastEventID int64
		ASMState    ASMState
	}

	ASMState int
)

const (
	ASMStateCreated ASMState = iota
	ASMStateRunning
	ASMStateClosed
	// ASMStateCorrupted
	// ASMStateZombie
)
