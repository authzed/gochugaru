package consistency

import (
	"context"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Strategy represents the strategy that a request can use in order to
// trade-off speed with latency.
// For more info see:
// https://authzed.com/docs/spicedb/concepts/consistency
// https://en.wikipedia.org/wiki/PACELC_theorem
type Strategy struct {
	V1Consistency *v1.Consistency
}

// WithOverlapKey creates a new context including the SpiceDB overlap key that
// will be used for subsequent requests.
func WithOverlapKey(ctx context.Context, key string) context.Context {
	return requestmeta.WithOverlapKey(ctx, key)
}

// Full configures the request to evaluate at the most recent
// revision of the database.
//
// This is the least performant, but guarantees read consistency.
func Full() *Strategy {
	return &Strategy{
		V1Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
		},
	}
}

// MinLatency configures the request to evaluate at the database's
// preferred revision.
//
// This provides the optimal performance and is the default if no consistency
// is specified.
func MinLatency() *Strategy {
	return &Strategy{
		V1Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{MinimizeLatency: true},
		},
	}
}

// AtLeast configures the request to evaluate at the provided
// database revision or newer.
//
// This should be used to avoid read-after-write inconsistencies.
func AtLeast(revision string) *Strategy {
	return &Strategy{
		V1Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &v1.ZedToken{Token: revision},
			},
		},
	}
}

// Snapshot configures the request to evaluate at the provided
// database revision.
//
// This should be very rarely used because SpiceDB is designed to pick the
// optimal revision for a request.
func Snapshot(revision string) *Strategy {
	return &Strategy{
		V1Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &v1.ZedToken{Token: revision},
			},
		},
	}
}
