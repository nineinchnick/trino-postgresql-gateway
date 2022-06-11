package main_test

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	pgx "github.com/jackc/pgx/v4"
	gw "github.com/nineinchnick/trino-postgresql-gateway"
	dt "github.com/ory/dockertest/v3"
)

var (
	pool     *dt.Pool
	resource *dt.Resource
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		var err error
		pool, err = dt.NewPool("")
		pool.MaxWait = 5 * time.Minute
		if err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}

		resource, err = pool.Run("trinodb/trino", "384", []string{})
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
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
	}

	code := m.Run()

	if pool != nil && resource != nil {
		// You can't defer this because os.Exit doesn't care for defer
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}

	os.Exit(code)
}

func TestServer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ready := make(chan struct{})
	go func() {
		err := gw.RunServer(ready, "localhost:15432", "http://test@localhost:"+resource.GetPort("8080/tcp"))
		if err != nil {
			t.Errorf("Server failed to run: %v", err)
		}
	}()
	<-ready

	assertQueries(t)
}

func TestServerWithMock(t *testing.T) {
	if !testing.Short() {
		t.Skip()
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	columns := []string{"name", "weight"}
	mock.ExpectQuery("select name, weight from widgets where id=42").
		WillReturnRows(sqlmock.NewRows(columns).FromCSVString("aaa,5"))

	ready := make(chan struct{})
	go func() {
		err = gw.RunServer(ready, "localhost:15432", "localhost:8080")
		if err != nil {
			t.Errorf("Server failed to run: %v", err)
		}
	}()
	<-ready

	assertQueries(t)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func assertQueries(t *testing.T) {
	conn, err := pgx.Connect(context.Background(), "postgres://some_user:some_pass@localhost:15432/memory?statement_cache_capacity=0")
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}
	defer conn.Close(context.Background())

	var name string
	var weight int64
	err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	if err != nil {
		t.Errorf("Failed to query for a row: %v", err)
	}
	if name != "aaa" {
		t.Errorf("Expected name=aaa but got: %s", name)
	}
	if weight != 5 {
		t.Errorf("Expected weight=5 but got: %d", weight)
	}
}
