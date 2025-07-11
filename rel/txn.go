package rel

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// Txn represents an atomic modification with option preconditions.
type Txn struct {
	V1Updates  []*v1.RelationshipUpdate
	V1Preconds []*v1.Precondition
}

// MustMatch modifies a transaction to only apply if the provided precondition
// is met.
func (b *Txn) MustMatch(f *Filter) {
	b.V1Preconds = append(b.V1Preconds, &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_MATCH,
		Filter:    f.V1Filter,
	})
}

// MustNotMatch modifies a transaction to only apply if the provided
// precondition is not met.
func (b *Txn) MustNotMatch(f *Filter) {
	b.V1Preconds = append(b.V1Preconds, &v1.Precondition{
		Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
		Filter:    f.V1Filter,
	})
}

// Touch idempotently creates or updates a relationship.
//
// The performance of this operation can vary based on the SpiceDB datastore.
func (b *Txn) Touch(r Relationship) {
	b.V1Updates = append(b.V1Updates, &v1.RelationshipUpdate{
		Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
		Relationship: r.V1(),
	})
}

// Create inserts a new relationship and fails if the relationship already
// exists.
func (b *Txn) Create(r Relationship) {
	b.V1Updates = append(b.V1Updates, &v1.RelationshipUpdate{
		Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
		Relationship: r.V1(),
	})
}

// Delete removes a relationship.
func (b *Txn) Delete(r Relationship) {
	b.V1Updates = append(b.V1Updates, &v1.RelationshipUpdate{
		Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
		Relationship: r.V1(),
	})
}
