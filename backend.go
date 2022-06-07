package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgproto3/v2"
	_ "github.com/trinodb/trino-go-client/trino"
)

type Backend struct {
	backend *pgproto3.Backend
	conn    net.Conn
	db      *sql.DB
}

func NewBackend(conn net.Conn, dsn string) (*Backend, error) {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	// TODO parse the dns into an URL and rebuild it with the auth obtained from the pg conn
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}

	connHandler := &Backend{
		backend: backend,
		conn:    conn,
		db:      db,
	}

	return connHandler, nil
}

func (p *Backend) Run() error {
	defer p.Close()

	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		rawMsg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.Query:
			// TODO run this in a goroutine and cancel its context on terminate
			err = p.handleQuery(msg)
			if err != nil {
				return fmt.Errorf("error handling a Query message: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (p *Backend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *Backend) handleQuery(msg *pgproto3.Query) error {
	// TODO don't use a timeout here but allow canceling the context when receiving a terminate (or close) message
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	query := strings.TrimRight(msg.String, ";")
	log.Println("Forwarding query", query)
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error executing a Trino query: %w", err)
	}
	defer rows.Close()
	// TODO figure out the response shape
	names := make([]string, 0)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			// Check for a scan error.
			// Query rows will be closed with defer.
			return fmt.Errorf("error scanning a Trino query response row: %w", err)
		}
		names = append(names, name)
	}
	// If the database is being written to ensure to check for Close
	// errors that may be returned from the driver. The query may
	// encounter an auto-commit error and be forced to rollback changes.
	rerr := rows.Close()
	if rerr != nil {
		return fmt.Errorf("error closing rows: %w", rerr)
	}

	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing rows: %w", err)
	}

	buf := (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{
			Name:                 []byte("fortune"),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		},
	}}).Encode(nil)
	response := "aaaaa"
	buf = (&pgproto3.DataRow{Values: [][]byte{[]byte(response)}}).Encode(buf)
	buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err = p.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("error writing query response: %w", err)
	}
	return nil
}

func (p *Backend) Close() error {
	dbErr := p.db.Close()
	connErr := p.conn.Close()
	if dbErr != nil {
		return dbErr
	}
	return connErr
}
