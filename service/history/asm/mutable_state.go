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

package asm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
)

const (
	initialDBRecordVersion = 0
)

type (
	MutableState interface {
		IsDirty() bool

		StartTransition() error
		CloseTransition() (persistence.ASMTransition, error)

		Token() ([]byte, error)
		ValidateToken([]byte) error

		Transit(
			newInstance plugin.Instance,
			newTasks []tasks.Task,
			newEvents []*historypb.HistoryEvent,
		)

		CurrentVersion() int64
		GetCurrentBranchToken() ([]byte, error)

		Execution() *persistencespb.ASMExecution
		ExecutionMetadata() *persistencespb.WorkflowExecutionState
		Plugin() plugin.Plugin
		PluginInstance() plugin.Instance
	}

	asmToken struct {
		ShardClock *clockspb.VectorClock

		// TODO: this should be stateTransitionHistoryItem
		VersionHistoryItem *historyspb.VersionHistoryItem
	}

	mutableStateImpl struct {
		execution         *persistencespb.ASMExecution
		executionMetadata *persistencespb.WorkflowExecutionState
		pluginInstance    plugin.Instance

		currentVersion  int64
		dbRecordVersion int64

		newTasks  map[tasks.Category][]tasks.Task
		newEvents [][]*historypb.HistoryEvent

		shard           shard.Context
		clusterMetadata cluster.Metadata
		eventsCache     events.Cache
		config          *configs.Config
		timeSource      clock.TimeSource
		logger          log.Logger
		metricsHandler  metrics.Handler
		namespaceEntry  *namespace.Namespace
		plugin          plugin.Plugin
	}
)

func NewMutableState(
	shard shard.Context,
	plugin plugin.Plugin,
	asmType string,
	asmKey definition.WorkflowKey,
	createRequestID string,
	namespaceEntry *namespace.Namespace,
	startTime time.Time,
) *mutableStateImpl {
	impl := &mutableStateImpl{
		execution: &persistencespb.ASMExecution{
			AsmType:              asmType,
			NamespaceId:          asmKey.NamespaceID,
			AsmId:                asmKey.WorkflowID,
			StartTime:            timestamppb.New(startTime),
			VersionHistories:     versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
			StateTransitionCount: 0,
			LastFirstEventTxnId:  0,
			NextEventId:          common.FirstEventID,
		},
		executionMetadata: &persistencespb.WorkflowExecutionState{
			CreateRequestId: createRequestID,
			RunId:           asmKey.RunID,
			State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},

		currentVersion:  namespaceEntry.FailoverVersion(),
		dbRecordVersion: initialDBRecordVersion,

		newTasks: make(map[tasks.Category][]tasks.Task),

		shard:           shard,
		clusterMetadata: shard.GetClusterMetadata(),
		eventsCache:     shard.GetEventsCache(),
		config:          shard.GetConfig(),
		timeSource:      shard.GetTimeSource(),
		logger:          shard.GetLogger(),
		metricsHandler:  shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		namespaceEntry:  namespaceEntry,
		plugin:          plugin,
	}

	branchToken, err := shard.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
		asmKey.NamespaceID,
		asmKey.WorkflowID,
		asmKey.RunID,
		asmKey.RunID,
		nil,
		[]*persistencespb.HistoryBranchRange{},
		0, // not used by cass
		0, // not used by cass
		0, // not used by cass
	)
	if err != nil {
		panic("cassandra persistence cannot generate branch token")
	}
	impl.setCurrentBranchToken(branchToken)
	return impl
}

func NewMutableStateFromDB(
	shard shard.Context,
	pluginRegistry Registry,
	namespaceEntry *namespace.Namespace,
	execution *persistencespb.ASMExecution,
	executionMetadata *persistencespb.WorkflowExecutionState,
	dbRecordVersion int64,
) (*mutableStateImpl, error) {
	impl := &mutableStateImpl{
		execution:         execution,
		executionMetadata: executionMetadata,
		// TODO: decode to user defined type
		// executionBody: nil,

		currentVersion:  namespaceEntry.FailoverVersion(),
		dbRecordVersion: dbRecordVersion,

		newTasks: make(map[tasks.Category][]tasks.Task),

		shard:           shard,
		clusterMetadata: shard.GetClusterMetadata(),
		eventsCache:     shard.GetEventsCache(),
		config:          shard.GetConfig(),
		timeSource:      shard.GetTimeSource(),
		logger:          shard.GetLogger(),
		metricsHandler:  shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		namespaceEntry:  namespaceEntry,
	}

	pluginName := execution.GetAsmType()
	pluginImpl := pluginRegistry.Get(pluginName)
	impl.plugin = pluginImpl

	executionBody, err := pluginImpl.Serializer().DeserializeState(execution.GetBody())
	if err != nil {
		return nil, err
	}

	restoreResponse, err := pluginImpl.Restore(context.TODO(), plugin.RestoreInstanceRequest{
		StateSnapshot: plugin.StateSnapshot{
			Core: executionBody,
		},
	})
	if err != nil {
		return nil, err
	}

	impl.pluginInstance = restoreResponse.Instance

	return impl, nil
}

func (m *mutableStateImpl) IsDirty() bool {
	// TODO: later
	return false
}

func (m *mutableStateImpl) CurrentVersion() int64 {
	return m.currentVersion
}

func (m *mutableStateImpl) StartTransition() error {
	// TODO: need proper logic for updating current version
	m.currentVersion = m.namespaceEntry.FailoverVersion()

	return nil
}

func (m *mutableStateImpl) CloseTransition() (persistence.ASMTransition, error) {
	// serialize state
	executionBody, err := m.plugin.Serializer().SerializeState(m.pluginInstance.StateSnapshot().Core)
	if err != nil {
		return persistence.ASMTransition{}, err
	}
	m.execution.Body = executionBody

	// prepare history events for persistence
	totalEvents := 0
	for _, eventBatch := range m.newEvents {
		totalEvents += len(eventBatch)
	}

	eventTxnIDs, err := m.shard.GenerateTaskIDs(totalEvents)
	if err != nil {
		return persistence.ASMTransition{}, err
	}

	currentEvent := 0
	for _, eventBatch := range m.newEvents {
		for _, event := range eventBatch {
			event.Version = m.currentVersion
			event.TaskId = eventTxnIDs[currentEvent]
			currentEvent++
		}
	}

	currentBranchToken, err := m.GetCurrentBranchToken()
	if err != nil {
		return persistence.ASMTransition{}, err
	}

	historyNodeTxnIDs, err := m.shard.GenerateTaskIDs(len(m.newEvents))
	if err != nil {
		return persistence.ASMTransition{}, err
	}

	var persistenceEventBatches []*persistence.WorkflowEvents
	for idx, eventBatch := range m.newEvents {
		persistenceEventBatches = append(persistenceEventBatches, &persistence.WorkflowEvents{
			NamespaceID: m.execution.NamespaceId,
			WorkflowID:  m.execution.AsmId,
			RunID:       m.executionMetadata.RunId,
			BranchToken: currentBranchToken,
			PrevTxnID:   m.execution.LastFirstEventTxnId,
			TxnID:       historyNodeTxnIDs[idx],
			Events:      eventBatch,
		})
		m.execution.LastFirstEventTxnId = historyNodeTxnIDs[idx]
	}

	if len(persistenceEventBatches) != 0 {
		lastBatch := persistenceEventBatches[len(persistenceEventBatches)-1]
		lastEvent := lastBatch.Events[len(lastBatch.Events)-1]
		m.updateWithLastWriteEvent(lastEvent)
	}

	m.dbRecordVersion += 1

	// prepare tasks for persistence
	newASMToken, err := m.Token()
	if err != nil {
		return persistence.ASMTransition{}, err
	}

	for _, taskBatch := range m.newTasks {
		for _, task := range taskBatch {
			task.(*tasks.ASMTask).ASMToken = newASMToken
		}
	}

	transition := persistence.ASMTransition{
		ExecutionMetadata: m.executionMetadata,
		Execution:         m.execution,
		Tasks:             m.newTasks,
		NewHistoryBatches: persistenceEventBatches,
		DBRecordVersion:   m.dbRecordVersion,
	}

	// clean up
	m.newTasks = make(map[tasks.Category][]tasks.Task)
	m.newEvents = nil

	return transition, nil
}

func (m *mutableStateImpl) Token() ([]byte, error) {
	// TODO: instead of using version history item,
	// should create a separate tree for state transition and
	// use state transition tree item.
	// Because not all state transition will have history event.

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(m.execution.VersionHistories)
	if err != nil {
		return nil, err
	}
	currentLastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, err
	}

	return encodeASMToken(asmToken{
		ShardClock:         m.shard.CurrentVectorClock(),
		VersionHistoryItem: currentLastItem,
	})
}

func (m *mutableStateImpl) ValidateToken(
	token []byte,
) error {
	if len(token) == 0 {
		return nil
	}

	asmToken, err := decodeASMToken(token)
	if err != nil {
		return serviceerror.NewInvalidArgument("invalid asm token")
	}

	clockVerified := false
	currentClock := m.shard.CurrentVectorClock()
	if vclock.Comparable(asmToken.ShardClock, currentClock) {
		result, err := vclock.Compare(asmToken.ShardClock, currentClock)
		if err != nil {
			return err
		}
		if result > 0 {
			shardID := m.shard.GetShardID()
			m.shard.UnloadForOwnershipLost()
			return &persistence.ShardOwnershipLostError{
				ShardID: shardID,
				Msg:     fmt.Sprintf("Shard: %v consistency check failed, reloading", shardID),
			}
		}
		clockVerified = true
	}

	onCurrentBranch, err := m.isVersionHistoryItemOnCurrentBranch(
		asmToken.VersionHistoryItem,
	)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok || clockVerified {
			return err
		}
		return consts.ErrStaleState
	}

	if !onCurrentBranch {
		return serviceerror.NewNotFound("request is for a different branch")
	}

	return nil
}

func (m *mutableStateImpl) Transit(
	newInstance plugin.Instance,
	newTasks []tasks.Task,
	newEvents []*historypb.HistoryEvent,
) {
	m.pluginInstance = newInstance
	for _, task := range newTasks {
		m.newTasks[task.GetCategory()] = append(m.newTasks[task.GetCategory()], task)
	}
	if len(newEvents) != 0 {
		m.newEvents = append(m.newEvents, newEvents)
		m.execution.NextEventId += int64(len(newEvents))
	}

	// TODO: update state & status?
}

func (m *mutableStateImpl) Execution() *persistencespb.ASMExecution {
	return m.execution
}

func (m *mutableStateImpl) ExecutionMetadata() *persistencespb.WorkflowExecutionState {
	return m.executionMetadata
}

func (m *mutableStateImpl) Plugin() plugin.Plugin {
	return m.plugin
}

func (m *mutableStateImpl) PluginInstance() plugin.Instance {
	return m.pluginInstance
}

func (m *mutableStateImpl) updateWithLastWriteEvent(
	lastEvent *historypb.HistoryEvent,
) error {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(m.execution.VersionHistories)
	if err != nil {
		return err
	}
	return versionhistory.AddOrUpdateVersionHistoryItem(currentVersionHistory, versionhistory.NewVersionHistoryItem(
		lastEvent.GetEventId(), lastEvent.GetVersion(),
	))
}

func (m *mutableStateImpl) GetCurrentBranchToken() ([]byte, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(m.execution.VersionHistories)
	if err != nil {
		return nil, err
	}
	return currentVersionHistory.GetBranchToken(), nil
}

func (m *mutableStateImpl) setCurrentBranchToken(
	branchToken []byte,
) error {

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(m.execution.VersionHistories)
	if err != nil {
		return err
	}
	versionhistory.SetVersionHistoryBranchToken(currentVersionHistory, branchToken)
	return nil
}

func (m *mutableStateImpl) isVersionHistoryItemOnCurrentBranch(
	item *historyspb.VersionHistoryItem,
) (bool, error) {

	versionHistoryies := m.execution.GetVersionHistories()
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistoryies)
	if err != nil {
		return false, err
	}

	onCurrentBranch := versionhistory.ContainsVersionHistoryItem(
		currentVersionHistory,
		item,
	)
	if onCurrentBranch {
		return true, nil
	}

	// check if on other branches
	if _, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		versionHistoryies,
		item,
	); err != nil {
		return false, &serviceerror.NotFound{Message: "History event not found"}
	}

	return false, nil
}

type asmTokenJSON struct {
	ShardClock         []byte
	VersionHistoryItem []byte
}

func encodeASMToken(
	token asmToken,
) ([]byte, error) {
	var asmTokenJSON asmTokenJSON

	var err error
	asmTokenJSON.ShardClock, err = token.ShardClock.Marshal()
	if err != nil {
		return nil, err
	}

	asmTokenJSON.VersionHistoryItem, err = token.VersionHistoryItem.Marshal()
	if err != nil {
		return nil, err
	}

	return json.Marshal(asmTokenJSON)
}

func decodeASMToken(
	data []byte,
) (asmToken, error) {

	var tokenJSON asmTokenJSON
	err := json.Unmarshal(data, &tokenJSON)
	if err != nil {
		return asmToken{}, err
	}

	var token asmToken
	token.ShardClock = &clockspb.VectorClock{}
	if err := token.ShardClock.Unmarshal(tokenJSON.ShardClock); err != nil {
		return asmToken{}, err
	}

	token.VersionHistoryItem = &historyspb.VersionHistoryItem{}
	if err := token.VersionHistoryItem.Unmarshal(tokenJSON.VersionHistoryItem); err != nil {
		return asmToken{}, err
	}

	return token, err
}
