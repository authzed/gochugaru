package client_test

import (
	"context"
	"fmt"
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
	return uuid.New().String()
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

func ExampleClient_FilterRelationships() {
	ctx := context.TODO()

	c, err := client.NewPlaintext("127.0.0.1:50051", randomPresharedKey())
	if err != nil {
		panic(err)
	}

	if _, err := c.WriteSchema(ctx, exampleSchema); err != nil {
		panic(err)
	}

	var txn rel.Txn
	txn.Create(rel.MustFromTriple("document:README", "reader", "user:jimmy"))
	if _, err := c.Write(ctx, &txn); err != nil {
		panic(err)
	}

	iter, err := c.FilterRelationships(
		ctx,
		consistency.MinLatency(),
		rel.NewFilter("document", "", ""),
	)
	if err != nil {
		panic(err)
	}

	for r, err := range iter {
		if err != nil {
			panic(err)
		}
		fmt.Println(r)
	}
	// Output:
	// document:README#reader@user:jimmy
}

func TestClient_Check(t *testing.T) {
	ctx := context.TODO()

	c, err := client.NewPlaintext("127.0.0.1:50051", randomPresharedKey())
	require.NoError(t, err, "Failed to create client")

	_, err = c.WriteSchema(ctx, exampleSchema)
	require.NoError(t, err, "Failed to write schema")

	var txn rel.Txn
	txn.Create(rel.MustFromTriple("document:check_test1", "writer", "user:alice"))
	txn.Create(rel.MustFromTriple("document:check_test1", "reader", "user:bob"))
	txn.Create(rel.MustFromTriple("document:check_test2", "writer", "user:charlie"))
	_, err = c.Write(ctx, &txn)
	require.NoError(t, err, "Failed to write relationships")

	t.Run("single relationship check - has permission", func(t *testing.T) {
		results, err := c.Check(ctx, consistency.MinLatency(),
			rel.MustFromTriple("document:check_test1", "edit", "user:alice"))
		require.NoError(t, err, "Check failed")
		require.Len(t, results, 1, "Expected 1 result")
		require.True(t, results[0], "Expected alice to have edit permission on document:check_test1")
	})

	t.Run("single relationship check - no permission", func(t *testing.T) {
		results, err := c.Check(ctx, consistency.MinLatency(),
			rel.MustFromTriple("document:check_test1", "edit", "user:bob"))
		require.NoError(t, err, "Check failed")
		require.Len(t, results, 1, "Expected 1 result")
		require.False(t, results[0], "Expected bob to not have edit permission on document:check_test1")
	})

	t.Run("multiple relationships check", func(t *testing.T) {
		results, err := c.Check(ctx, consistency.MinLatency(),
			rel.MustFromTriple("document:check_test1", "edit", "user:alice"),
			rel.MustFromTriple("document:check_test1", "view", "user:bob"),
			rel.MustFromTriple("document:check_test2", "edit", "user:charlie"),
			rel.MustFromTriple("document:check_test2", "view", "user:alice"))
		require.NoError(t, err, "Check failed")
		require.Len(t, results, 4, "Expected 4 results")

		expected := []bool{true, true, true, false}
		for i, expectedResult := range expected {
			require.Equal(t, expectedResult, results[i], "Result %d mismatch", i)
		}
	})

	t.Run("different consistency strategies", func(t *testing.T) {
		strategies := []*consistency.Strategy{
			consistency.MinLatency(),
			consistency.Full(),
		}

		for _, strategy := range strategies {
			results, err := c.Check(ctx, strategy,
				rel.MustFromTriple("document:check_test1", "edit", "user:alice"))
			require.NoError(t, err, "Check with consistency strategy failed")
			require.Len(t, results, 1, "Expected 1 result")
			require.True(t, results[0], "Expected alice to have edit permission on document:check_test1")
		}
	})

	t.Run("empty relationships", func(t *testing.T) {
		results, err := c.Check(ctx, consistency.MinLatency())
		require.NoError(t, err, "Check with no relationships failed")
		require.Len(t, results, 0, "Expected 0 results")
	})

	t.Run("nonexistent resource", func(t *testing.T) {
		results, err := c.Check(ctx, consistency.MinLatency(),
			rel.MustFromTriple("document:nonexistent", "edit", "user:alice"))
		require.NoError(t, err, "Check failed")
		require.Len(t, results, 1, "Expected 1 result")
		require.False(t, results[0], "Expected no permission on nonexistent resource")
	})
}
