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

import "context"

type (
	// Engine is the interface of the framework that executes ASM.
	// This is not today's history engine which bind to the life cycle of a history shard.
	// It's more like today's history handler.
	Engine interface {
		Start(context.Context, StartRequest) (StartResponse, error)
		// TransitWithStart(context.Context, TransitWithStartRequest) (TransitWithStartResponse, error)
		Transit(context.Context, TransitRequest) (TransitResponse, error)
		// Poll(context.Context, PollRequest) (PollResponse, error)
		// Reset(context.Context, ResetRequest) (ResetResponse, error)
		// Terminate(context.Context, TerminateRequest) (TerminateResponse, error)
		// Delete(context.Context, DeleteRequest) (DeleteResponse, error)

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
		ASMToken []byte

		ASMStartResponse any
	}

	// TransitWithStartRequest struct {
	// 	ASMType string

	// 	NamespaceID string

	// 	ID            string
	// 	IDReusePolicy IDReusePolicy

	// 	RequestID string

	// 	ASMStartRequest   any
	// 	ASMTransitRequest any
	// }

	// TransitWithStartResponse struct {
	// 	ASMToken ASMContext

	// 	ASMStartResponse   any
	// 	ASMTransitResponse any
	// }

	TransitRequest struct {
		ASMToken []byte
		ASMRef   ASMRef

		ASMTransitRequest any
	}

	TransitResponse struct {
		ASMToken []byte

		ASMTransitResponse any
	}

	// PollRequest struct {
	// 	ASMToken ASMContext
	// 	ASMRef     ASMRef

	// 	ASMCondition  ASMCondition
	// 	ASMStateQuery ASMStateQuery
	// 	PollToken     []byte

	// 	ASMPollRequest any
	// }

	// PollResponse struct {
	// 	ASMToken ASMContext
	// 	PollToken  []byte

	// 	ASMPollResponse any
	// }

	// ResetRequest struct {
	// 	ASMToken ASMContext
	// 	ASMRef     ASMRef

	// 	RequestID                    string
	// 	HistoryEventReapplyPredicate HistoryEventReapplyPredicate
	// }

	// HistoryEventReapplyPredicate func(event HistoryEvent) bool

	// ResetResponse struct {
	// 	ASMToken  ASMContext
	// 	ResetASMKey ASMKey
	// }

	// TerminateRequest struct {
	// 	// TODO: do we need this for terminate?
	// 	ASMToken ASMContext
	// 	ASMRef     ASMRef

	// 	ASMTerminateRequest any
	// }

	// TerminateResponse struct {
	// 	ASMToken ASMContext

	// 	ASMTerminateResponse any
	// }

	// DeleteRequest struct {
	// 	// TODO: do we need this for delete?
	// 	ASMToken ASMContext
	// 	ASMRef     ASMRef
	// }

	// DeleteResponse struct{}
)

type (
	// ASMKey struct {
	// 	NamespaceID string
	// 	ID          string
	// 	RunID       string
	// }

	ASMRef struct {
		NamespaceID string
		ID          string

		// At most one of the following fields can be set.
		// Optional
		RunID string
		// Optional
		// ChainFirstRunID string
	}

	// ASMToken []byte

	IDReusePolicy int

	// ASMCondition struct {
	// 	ASMEventCondition ASMEventCondition
	// 	ASMStateCondition ASMStateCondition
	// }

	// ASMEventCondition struct {
	// 	TargetEventID int64
	// }

	// ASMStateCondition struct {
	// 	StatePredicate func(ASMState any) bool
	// }

	// ASMStateQuery func(ASMMetadata ASMMetadata, ASMState any) any

	// ASMMetadata struct {
	// 	ASMKey      ASMKey
	// 	LastEventID int64
	// 	ASMState    ASMState
	// }

	// ASMState int
)

const (
	IDReusePolicyRejectDuplicate IDReusePolicy = iota
)

const (
// ASMStateCreated ASMState = iota
// ASMStateRunning
// ASMStateClosed
// ASMStateCorrupted
// ASMStateZombie
)
