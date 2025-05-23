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
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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
	return "somerandomkeyhere"
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
