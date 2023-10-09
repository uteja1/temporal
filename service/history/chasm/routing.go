package chasm

import (
	"go.temporal.io/server/common/membership"
)

type (
	RoutingScheme int

	RoutingKey struct {
		NamespaceID string
		ASMID       string
	}

	Router interface {
		Route(RoutingKey) (string, error) // hostAddress, error
	}
)

const (
	RoutingSchemeShard RoutingScheme = iota
	RoutingSchemeWedge
)

func NewShardRouter(
	ASMServiceResolver membership.ServiceResolver,
) Router {
	return nil
}
