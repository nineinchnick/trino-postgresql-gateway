package main_test

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	pgx "github.com/jackc/pgx/v4"
	gw "github.com/nineinchnick/trino-postgresql-gateway"
)

func TestServer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	// TODO run a Trino container
	ready := make(chan struct{})
	go func() {
		err := gw.RunServer(ready, "localhost:15432", "localhost:8080")
		if err != nil {
			t.Errorf("Server failed to run: %v", err)
		}
	}()
	<-ready

	// TODO run a query
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

	conn, err := pgx.Connect(context.Background(), "postgres://some_user:some_pass@localhost:15432/memory")
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

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
