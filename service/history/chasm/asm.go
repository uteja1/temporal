package chasm

import (
	"context"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	ASM interface {
		Type() string

		Configs() ASMConfigs
		TaskExecutor() TaskExecutor
		TaskVerifier() TaskVerifier // replication only
		Serializer() Serializer

		Start(context.Context, StartInstanceRequest) (StartInstanceResponse, error)
		Restore(context.Context, RestoreInstanceRequest) (RestoreInstanceResponse, error)
		Reset(context.Context, ResetInstanceRequest) (ResetInstanceResponse, error)
	}

	ASMConfigs struct {
		RoutingScheme RoutingScheme

		// DisableVisibility  bool
		DisablePersistence        bool
		DisableReplication        bool
		DisableHistoryArchival    bool // shall we support different archival URI for different ASM?
		DisableVisibilityArchival bool
	}

	Instance interface {
		Transit(context.Context, TransitInstanceRequest) (TransitInstanceResponse, error)

		IsRunning() bool // execution state
		IsFailed() bool  // execution close status

		Key() ASMKey
		StateSnapshot() StateSnapshot // part of state that needs to be persisted, it's ok to have in memory only state
		SearchAttributes() SearchAttributes

		// we can make this more granular if needed
		// like IDRetention, VisibilityRetention, HistoryRetention etc.
		Retention() time.Duration
	}
)

type (
	// ASMResources should be injected via fx into concrete ASM implementations
	// NamespaceRegistry
	// ClusterMetadata

	StartInstanceRequest struct {
		Key ASMKey

		ASMStartRequest any
	}

	StartInstanceResponse struct {
		NewInstanceTransition InstanceTransition

		ASMStartResponse any
	}

	RestoreInstanceRequest struct {
		StateSnapshot StateSnapshot
	}

	RestoreInstanceResponse struct {
		Instance Instance
	}

	ResetInstanceRequest struct {
		BaseInstantceKey ASMKey
		ResetInstanceKey ASMKey

		HistoryEventsIterator        HistoryEventsIterator
		ReapplyHistoryEventsIterator HistoryEventsIterator

		Reason string
	}

	ResetInstanceResponse struct {
		// HistoryEvents should only include events after HistoryEventsIterator
		NewInstanceTransition InstanceTransition
	}

	StateSnapshot struct {
		Core any

		// subStateType -> (subStateID -> subState)
		SubStates map[string]map[string]any
	}

	StateMutation struct {
		Core any

		// subStateType -> (subStateID -> subState)
		UpsertSubStates map[string]map[string]any
		DeleteSubStates map[string]map[string]struct{}
	}

	// TODO: specify the list of accepted search attribute value types
	SearchAttributes map[string]interface{}
)

type (
	TransitResources struct {
		Metadata                   InstanceMetadata
		HistoryEventTokenGenerator HistoryEventTokenGenerator

		Logger         log.Logger
		MetricsHandler metrics.Handler
	}

	InstanceMetadata struct {
		StartTime time.Time
		CloseTime time.Time

		HistoryEventCount    int64
		HistoryEventBytes    int64
		StateTransitionCount int64
		StateSnapshotBytes   int64

		// add more here if needed
	}

	TransitInstanceRequest struct {
		RequestASMContext ASMContext
		CurrentASMContext ASMContext
		// NewASMContext     ASMContext
		Resources TransitResources

		// this will be the key for the new instance if NewInstanceStartRequest is specified in the response
		// why do we need this? Current run want to know the runID of the new run.
		NewInstanceKey ASMKey

		Event Event
	}

	TransitInstanceResponse struct {
		CurrentInstanceTransition *InstanceTransition

		// if not nil, a new instance will be created (via ASM.Start())
		NewInstanceStartRequest any

		ASMTransitResponse any
	}

	InstanceTransition struct {
		// Instance object after the transition
		// this can be the same object as the one used to make the transition
		Instance Instance

		// optional: if Mutation is returned, it will be used to update persisted state of the ASM
		// implementation need to make sure after Mutation is applied, state in DB matches the state in memory
		Mutation *StateMutation

		HistoryEvents []HistoryEvent
		Tasks         []Task

		// default: false
		// Ignored if DisableVisibility is true when registering the ASM
		UpsertVisibility bool

		// default: false
		// if true, Mutation, HistoryEvents & Tasks must be empty. UpsertVisibility must be false
		// Ignored if DisablePersistence is true when registering the ASM
		SkipPersistence bool
	}
)
