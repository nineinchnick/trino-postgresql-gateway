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
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
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

	columns, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("error getting Trino query response columns: %w", err)
	}
	fields := make([]pgproto3.FieldDescription, len(columns))
	vals := make([]interface{}, len(columns))
	for i, column := range columns {
		// TODO map types to OIDs and size
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(column.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          25,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
		vals[i] = new(interface{})
	}
	buf := (&pgproto3.RowDescription{Fields: fields}).Encode(nil)

	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return fmt.Errorf("error scanning a Trino query response row: %w", err)
		}

		dataRow, err := buildDataRow(vals, columns)
		if err != nil {
			return fmt.Errorf("error building data row: %w", err)
		}
		buf = dataRow.Encode(buf)
	}
	rerr := rows.Close()
	if rerr != nil {
		return fmt.Errorf("error closing rows: %w", rerr)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing rows: %w", err)
	}

	buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
	buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	_, err = p.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("error writing query response: %w", err)
	}
	return nil
}

func buildDataRow(values []interface{}, columns []*sql.ColumnType) (*pgproto3.DataRow, error) {
	dr := &pgproto3.DataRow{
		Values: make([][]byte, len(values)),
	}

	for i, rawVal := range values {
		var backendVal interface{}
		ptr, ok := rawVal.(*interface{})
		if !ok {
			return nil, errors.Errorf("values[%d] (%+v) expected to be a pointer", i, columns[i])
		}
		switch v := (*ptr).(type) {
		case string:
			backendVal = &pgtype.Text{String: v, Status: pgtype.Present}
		case int16:
			backendVal = &pgtype.Int2{Int: v, Status: pgtype.Present}
		case int32:
			backendVal = &pgtype.Int4{Int: v, Status: pgtype.Present}
		case int64:
			backendVal = &pgtype.Int8{Int: v, Status: pgtype.Present}
		case float32:
			backendVal = &pgtype.Float4{Float: v, Status: pgtype.Present}
		case float64:
			backendVal = &pgtype.Float8{Float: v, Status: pgtype.Present}
		default:
			return nil, errors.Errorf("Unknown scan type %T", v)
		}

		enc, ok := backendVal.(pgtype.TextEncoder)
		if ok {
			buf, err := enc.EncodeText(nil, nil)
			if err != nil {
				return nil, errors.Errorf("failed to encode values[%d] (%+v)", i, columns[i])
			}
			dr.Values[i] = buf
			continue
		}
		benc, ok := backendVal.(pgtype.BinaryEncoder)
		if ok {
			buf, err := benc.EncodeBinary(nil, nil)
			if err != nil {
				return nil, errors.Errorf("failed to encode values[%d] (%+v)", i, columns[i])
			}
			dr.Values[i] = buf
			continue
		}
		return nil, errors.Errorf("values[%d] (%+v) does not implement TextExcoder or BinaryEncoder", i, columns[i])
	}

	return dr, nil
}

func (p *Backend) Close() error {
	dbErr := p.db.Close()
	connErr := p.conn.Close()
	if dbErr != nil {
		return dbErr
	}
	return connErr
}
