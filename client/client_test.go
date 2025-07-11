package client_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"testing"
	"time"

	"github.com/authzed/gochugaru/client"
	"github.com/authzed/gochugaru/consistency"
	"github.com/authzed/gochugaru/rel"
	"github.com/authzed/grpcutil"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func ExampleClient_ReadRelationships() {
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
	if _, err := c.Write(ctx, txn); err != nil {
		panic(err)
	}

	iter := c.ReadRelationships(
		ctx,
		consistency.MinLatency(),
		rel.NewFilter("document", "", ""),
	)

	for r, err := range iter {
		if err != nil {
			panic(err)
		}
		fmt.Println(r)
	}
	// Output:
	// document:README#reader@user:jimmy
}

func TestClient_LookupResources(t *testing.T) {
	ctx := context.TODO()

	c, err := client.NewPlaintext("127.0.0.1:50051", randomPresharedKey())
	require.NoError(t, err, "Failed to create client")

	_, err = c.WriteSchema(ctx, exampleSchema)
	require.NoError(t, err, "Failed to write schema")

	var txn rel.Txn
	txn.Create(rel.MustFromTriple("document:check_test1", "writer", "user:alice"))
	txn.Create(rel.MustFromTriple("document:check_test1", "reader", "user:bob"))
	txn.Create(rel.MustFromTriple("document:check_test1", "writer", "user:charlie"))
	txn.Create(rel.MustFromTriple("document:check_test2", "writer", "user:charlie"))
	_, err = c.Write(ctx, txn)
	require.NoError(t, err, "Failed to write relationships")

	t.Run("lookup resources - document objects returned", func(t *testing.T) {
		for id, err := range c.LookupResources(ctx, consistency.Full(), "document#writer", "user:alice") {
			require.NoError(t, err, "Lookup failed")
			require.Equal(t, "check_test1", id)
		}
	})
	t.Run("lookup resources - multiple objects returned", func(t *testing.T) {
		var ids []string
		for id, err := range c.LookupResources(ctx, consistency.Full(), "document#writer", "user:charlie") {
			require.NoError(t, err, "Lookup failed")
			ids = append(ids, id)
		}
		slices.Sort(ids)
		require.Equal(t, []string{"check_test1", "check_test2"}, ids)
	})
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
	_, err = c.Write(ctx, txn)
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

func TestMissingOverlapPanic(t *testing.T) {
	ctx := context.TODO()

	c, err := client.NewWithOpts("127.0.0.1:50051",
		client.WithOverlapRequired(),
		client.WithDialOpts(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpcutil.WithInsecureBearerToken(randomPresharedKey()),
		),
	)
	require.NoError(t, err, "Failed to create client")

	_, err = c.WriteSchema(ctx, exampleSchema)
	require.NoError(t, err, "Failed to write schema")

	t.Run("Missing Overlap Panics - ReadRelationships", func(t *testing.T) {
		defer func() { _ = recover() }()
		_ = c.ReadRelationships(ctx, consistency.Full(), rel.NewFilter("document", "", ""))
		t.Fatal("did not panic when overlap not provided")
	})
	t.Run("Missing Overlap Panics - ExportRelationships", func(t *testing.T) {
		defer func() { _ = recover() }()
		_ = c.ExportRelationships(ctx, "")
		t.Fatal("did not panic when overlap not provided")
	})

	t.Run("Missing Overlap Panics - Check", func(t *testing.T) {
		defer func() { _ = recover() }()
		_, _ = c.CheckOne(ctx, consistency.Full(), rel.MustFromTriple("document:README", "owner", "user:bot"))
		t.Fatal("did not panic when overlap not provided")
	})

	t.Run("Missing Overlap Panics - DeleteAtomic", func(t *testing.T) {
		defer func() { _ = recover() }()
		_, _ = c.DeleteAtomic(ctx, rel.NewPreconditionedFilter(rel.NewFilter("document", "", "")))
		t.Fatal("did not panic when overlap not provided")
	})
	t.Run("Missing Overlap Panics - Delete", func(t *testing.T) {
		defer func() { _ = recover() }()
		_ = c.Delete(ctx, rel.NewPreconditionedFilter(rel.NewFilter("document", "", "")))
		t.Fatal("did not panic when overlap not provided")
	})

	t.Run("Missing Overlap Panics - Updates", func(t *testing.T) {
		defer func() { _ = recover() }()
		_ = c.Updates(ctx, rel.UpdateFilter{})
		t.Fatal("did not panic when overlap not provided")
	})

	t.Run("Missing Overlap Panics - LookupResources", func(t *testing.T) {
		defer func() { _ = recover() }()
		_ = c.LookupResources(ctx, consistency.Full(), "document#writer", "user:alice")
		t.Fatal("did not panic when overlap not provided")
	})

	t.Run("Provided Overlap Doesn't Panic", func(t *testing.T) {
		_, _ = c.CheckOne(consistency.WithOverlapKey(ctx, "test"),
			consistency.Full(), rel.MustFromTriple("document:README", "owner", "user:bot"))
	})
}
