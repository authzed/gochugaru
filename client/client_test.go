package client_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/authzed/gochugaru/client"
	"github.com/authzed/gochugaru/consistency"
	"github.com/authzed/gochugaru/rel"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

const exampleSchema = `
definition user {}
definition document {
    relation writer: user
    relation reader: user

    permission edit = writer
    permission view = reader + edit
}
`

func randomPresharedKey() string {
	return "bearer " + uuid.New().String()
}

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not construct pool: %s", err)
	}

	if err = pool.Client.Ping(); err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Hostname:     "localhost:50051",
		Repository:   "authzed/spicedb",
		Tag:          "latest",
		Cmd:          []string{"serve-testing"},
		ExposedPorts: []string{"50051"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"50051": {{HostIP: "0.0.0.0", HostPort: "50051"}},
		},
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	time.Sleep(10 * time.Second)

	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}()

	m.Run()
}

func TestLookupResources(t *testing.T) {
	ctx := context.TODO()

	c, err := client.NewPlaintext("127.0.0.1:50051", randomPresharedKey())
	require.NoError(t, err)

	// Write the schema to the database.
	_, err = c.WriteSchema(ctx, exampleSchema)
	require.NoError(t, err)

	// Write some relationships to the database.
	var txn rel.Txn
	txn.Create(rel.MustFromTriple("document:readme", "writer", "user:jimmy"))
	txn.Create(rel.MustFromTriple("document:notes", "reader", "user:jimmy"))
	txn.Create(rel.MustFromTriple("document:readme", "reader", "user:tommy"))

	_, err = c.Write(ctx, &txn)
	require.NoError(t, err)

	// Perform paginated lookup resources.
	it := c.PaginatedLookupResources(ctx, consistency.Full(), "document", "view", rel.Subject{
		Type: "user",
		ID:   "jimmy",
	}, 1)

	results := make([]client.FoundResource, 0)
	for fr, err := range it {
		require.NoError(t, err)
		results = append(results, fr)
	}

	require.Len(t, results, 2)
	foundResourceIDs := make(map[string]struct{})
	for _, fr := range results {
		resourceID, isFullResult := fr.ResourceObjectID()
		require.True(t, isFullResult, "Expected full resource object ID")
		foundResourceIDs[resourceID] = struct{}{}
	}

	// Check that we found the expected resources.
	require.True(t, len(foundResourceIDs) == 2, "Expected to find two unique resources")
	require.Contains(t, foundResourceIDs, "readme")
	require.Contains(t, foundResourceIDs, "notes")
}

func TestFilterRelationships(t *testing.T) {
	ctx := context.TODO()

	c, err := client.NewPlaintext("127.0.0.1:50051", randomPresharedKey())
	require.NoError(t, err)

	// Write the schema to the database.
	_, err = c.WriteSchema(ctx, exampleSchema)
	require.NoError(t, err)

	var txn rel.Txn
	txn.Create(rel.MustFromTriple("document:README", "reader", "user:jimmy"))

	_, err = c.Write(ctx, &txn)
	require.NoError(t, err)

	iter, err := c.FilterRelationships(
		ctx,
		consistency.MinLatency(),
		rel.NewFilter("document", "", ""),
	)
	require.NoError(t, err)

	for _, err := range iter {
		require.NoError(t, err)
	}
}
