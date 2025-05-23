package rel_test

import (
	"fmt"
	"testing"

	"github.com/authzed/gochugaru/rel"
)

func TestRelationshipParsingTriples(t *testing.T) {
	cases := []struct {
		name, resource, relation, subject string
		expectedErr                       error
	}{
		{"valid rel", "document:example", "viewer", "user:jzelinskie", nil},
		{"missing resource", "", "viewer", "user:jzelinskie", rel.ErrInvalidResource},
		{"missing relation", "document:example", "", "user:jzelinskie", rel.ErrInvalidRelation},
		{"missing subject", "document:example", "viewer", "", rel.ErrInvalidSubject},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := rel.FromTriple(c.resource, c.relation, c.subject); err != c.expectedErr {
				t.Fatal(err)
			}
		})
	}
}

func ExampleMustFromTriple() {
	r := rel.MustFromTriple("document:example", "viewer", "user:jzelinskie")
	fmt.Println(r)
	// Output:
	// document:example#viewer@user:jzelinskie
}

func ExampleRelationship_WithCaveat() {
	fmt.Println(rel.
		MustFromTriple("document:example", "viewer", "user:jzelinskie").
		WithCaveat("only_on_tuesday", map[string]any{"day_of_the_week": "wednesday"}),
	)
	// Output:
	// document:example#viewer@user:jzelinskie[only_on_tuesday:{"day_of_the_week":"wednesday"}]
}
