package rel_test

import (
	"fmt"
	"testing"
	"time"

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

func ExampleRelationship_WithExpiration() {
	expiry := time.Date(2024, 12, 25, 15, 30, 0, 0, time.UTC)
	fmt.Println(rel.
		MustFromTriple("document:example", "viewer", "user:jzelinskie").
		WithExpiration(expiry),
	)
	// Output:
	// document:example#viewer@user:jzelinskie[expiration:2024-12-25T15:30:00Z]
}

func TestRelationshipExpiration(t *testing.T) {
	cases := []struct {
		name              string
		expiration        *time.Time
		expectedHasExp    bool
		expectedFormatted string
	}{
		{
			name:              "no expiration",
			expiration:        nil,
			expectedHasExp:    false,
			expectedFormatted: "document:example#viewer@user:jzelinskie",
		},
		{
			name:              "zero time expiration",
			expiration:        &time.Time{},
			expectedHasExp:    false,
			expectedFormatted: "document:example#viewer@user:jzelinskie",
		},
		{
			name:              "valid expiration",
			expiration:        func() *time.Time { t := time.Date(2024, 12, 25, 15, 30, 0, 0, time.UTC); return &t }(),
			expectedHasExp:    true,
			expectedFormatted: "document:example#viewer@user:jzelinskie[expiration:2024-12-25T15:30:00Z]",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := rel.MustFromTriple("document:example", "viewer", "user:jzelinskie")
			if c.expiration != nil {
				r = r.WithExpiration(*c.expiration)
			}

			if r.HasExpiration() != c.expectedHasExp {
				t.Errorf("expected HasExpiration() to be %v, got %v", c.expectedHasExp, r.HasExpiration())
			}

			if r.String() != c.expectedFormatted {
				t.Errorf("expected formatted string to be %q, got %q", c.expectedFormatted, r.String())
			}
		})
	}
}
