// Package main_test runs integration tests for the main package
// on a real server running in a container. During development, to avoid rebuilding
// containers every run, add the `-cleanup=false` flags when calling `go test`.
package main_test

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v4"
	gw "github.com/nineinchnick/trino-postgresql-gateway"
	dt "github.com/ory/dockertest/v3"
)

var (
	pool     *dt.Pool
	resource *dt.Resource

	cleanup bool
)

func TestMain(m *testing.M) {
	flag.BoolVar(&cleanup, "cleanup", true, "delete containers when finished")
	flag.Parse()
	var err error
	pool, err = dt.NewPool("")
	pool.MaxWait = 5 * time.Minute
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	name := "trino-postgresql-gatewayy-tests"
	var ok bool
	resource, ok = pool.ContainerByName(name)

	if !ok {
		resource, err = pool.RunWithOptions(&dt.RunOptions{
			Name:       name,
			Repository: "trinodb/trino",
			Tag:        "384",
		})
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}
	}

	if err := pool.Retry(func() error {
		c, err := pool.Client.InspectContainer(resource.Container.ID)
		if err != nil {
			return err
		}
		if c.State.Health.Status != "healthy" {
			return errors.New("Not ready")
		}
		return nil
	}); err != nil {
		log.Fatalf("Timed out waiting for container to get ready: %s", err)
	}

	code := m.Run()

	if cleanup && pool != nil && resource != nil {
		// You can't defer this because os.Exit doesn't care for defer
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	os.Exit(code)
}

func TestServer(t *testing.T) {
	ready := make(chan struct{})
	go func() {
		err := gw.RunServer(ready, "localhost:15432", "http://test@localhost:"+resource.GetPort("8080/tcp")+"?catalog=tpch&schema=tiny&custom_client=uncompressed")
		if err != nil {
			t.Errorf("Server failed to run: %v", err)
		}
	}()
	<-ready

	assertQueries(t)
}

func assertQueries(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://some_user:some_pass@localhost:15432/memory?statement_cache_capacity=0")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(context.Background())

	var name string
	var region int64
	err = conn.QueryRow(context.Background(), "select name, regionkey AS num from nation where nationkey=$1", 12).Scan(&name, &region)
	if err != nil {
		t.Errorf("Failed to query for a row: %v", err)
	}
	if name != "JAPAN" {
		t.Errorf("Expected name=JAPAN but got: %s", name)
	}
	if region != 2 {
		t.Errorf("Expected region=2 but got: %d", region)
	}
}
