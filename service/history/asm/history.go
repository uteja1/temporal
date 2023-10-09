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

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/asm/plugin"
	"go.temporal.io/server/service/history/events"
)

type (
	historyEventTokenGeneratorImpl struct {
		nodeID         int64
		currentVersion int64
	}

	historyEventLoaderImpl struct {
		shardID            int32
		eventsCache        events.Cache
		asmKey             definition.WorkflowKey
		currentBranchToken []byte
		pluginSerializer   plugin.Serializer
	}

	loadEventToken struct {
		NodeID  int64
		EventID int64
		Version int64
	}

	historyEventsIteratorImpl struct {
		shardID            int32
		currentBranchToken []byte
		executionManager   persistence.ExecutionManager
		pluginSerializer   plugin.Serializer

		iterator collection.Iterator[plugin.HistoryEvent]
	}
)

func (t *loadEventToken) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

func (t *loadEventToken) Deserialize(bytes []byte) error {
	return json.Unmarshal(bytes, t)
}

func newHistoryEventTokenGenerator(
	nodeID int64,
	currentVersion int64,
) plugin.HistoryEventTokenGenerator {
	return &historyEventTokenGeneratorImpl{
		nodeID:         nodeID,
		currentVersion: currentVersion,
	}
}

func (g *historyEventTokenGeneratorImpl) Generate(eventID int64) []byte {
	token := &loadEventToken{
		NodeID:  g.nodeID,
		EventID: int64(eventID),
		Version: g.currentVersion,
	}
	encodedToken, _ := token.Serialize()
	return encodedToken
}

func newHistoryEventLoader(
	shardID int32,
	eventsCache events.Cache,
	asmKey definition.WorkflowKey,
	currentBranchToken []byte,
	pluginSerializer plugin.Serializer,
) plugin.HistoryEventLoader {
	return &historyEventLoaderImpl{
		shardID:            shardID,
		eventsCache:        eventsCache,
		asmKey:             asmKey,
		currentBranchToken: currentBranchToken,
		pluginSerializer:   pluginSerializer,
	}
}

func (l *historyEventLoaderImpl) Load(
	ctx context.Context,
	loadRequest plugin.LoadHistoryEventRequest,
) (plugin.LoadHistoryEventResponse, error) {
	var token loadEventToken
	if err := token.Deserialize(loadRequest.HistoryEventToken); err != nil {
		return plugin.LoadHistoryEventResponse{}, err
	}

	event, err := l.eventsCache.GetEvent(
		ctx,
		l.shardID,
		events.EventKey{
			NamespaceID: namespace.ID(l.asmKey.NamespaceID),
			WorkflowID:  l.asmKey.WorkflowID,
			RunID:       l.asmKey.RunID,
			EventID:     token.EventID,
			Version:     token.Version,
		},
		token.NodeID,
		l.currentBranchToken,
	)
	if err != nil {
		return plugin.LoadHistoryEventResponse{}, err
	}

	pluginEvent, err := l.pluginSerializer.DeserializeHistoryEvent(event.GetBody())
	if err != nil {
		return plugin.LoadHistoryEventResponse{}, err
	}

	return plugin.LoadHistoryEventResponse{
		HistoryEvent: pluginEvent,
	}, nil
}

func newHistoryEventsIterator(
	shardID int32,
	currentBranchToken []byte,
	executionManager persistence.ExecutionManager,
	pluginSerializer plugin.Serializer,
) plugin.HistoryEventsIterator {

	return &historyEventsIteratorImpl{
		shardID:            shardID,
		currentBranchToken: currentBranchToken,
		executionManager:   executionManager,
		pluginSerializer:   pluginSerializer,
		iterator:           nil,
	}
}

func (i *historyEventsIteratorImpl) Next() (plugin.HistoryEvent, error) {
	return i.iterator.Next()
}

func (i *historyEventsIteratorImpl) HasNext() bool {
	if i.iterator == nil {
		i.iterator = collection.NewPagingIterator[plugin.HistoryEvent](
			func(paginationToken []byte) ([]plugin.HistoryEvent, []byte, error) {
				resp, err := i.executionManager.ReadHistoryBranch(
					context.TODO(),
					&persistence.ReadHistoryBranchRequest{
						ShardID:       i.shardID,
						BranchToken:   i.currentBranchToken,
						MinEventID:    common.FirstEventID,
						MaxEventID:    common.EndEventID,
						PageSize:      1000,
						NextPageToken: paginationToken,
					},
				)
				if err != nil {
					return nil, nil, err
				}

				decodedEvents := make([]plugin.HistoryEvent, 0, len(resp.HistoryEvents))
				for _, event := range resp.HistoryEvents {
					decodedEvent, err := i.pluginSerializer.DeserializeHistoryEvent(event.GetBody())
					if err != nil {
						return nil, nil, err
					}
					decodedEvents = append(decodedEvents, decodedEvent)
				}

				return decodedEvents, resp.NextPageToken, nil
			},
		)
	}

	return i.iterator.HasNext()
}
