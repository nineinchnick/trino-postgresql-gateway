package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
	"github.com/trinodb/trino-go-client/trino"
)

// Backend represents a client session
type Backend struct {
	backend    *pgproto3.Backend
	conn       net.Conn
	connInfo   *pgtype.ConnInfo
	ctx        context.Context
	ctxCancel  context.CancelFunc
	db         *sql.DB
	stmtQuery  string
	stmt       *sql.Stmt
	stmtTypes  []uint32
	stmtParams []interface{}
	stmtRows   *sql.Rows
}

// SoftError is recoverable and will be returned to the client as an ErrorResponse
type SoftError struct {
	Code string
	Err  error
}

func (e *SoftError) Error() string {
	return e.Err.Error()
}

func (e *SoftError) Unwrap() error {
	return e.Err
}

func newSoftError(code string, err error) *SoftError {
	return &SoftError{
		Code: code,
		Err:  err,
	}
}

var (
	NotImplementedErr = errors.New("not implemented")

	trinoToPg = map[string]uint32{
		"unknown":   pgtype.UnknownOID,
		"boolean":   pgtype.BoolOID,
		"varchar":   pgtype.VarcharOID,
		"tinyint":   pgtype.Int2OID,
		"smallint":  pgtype.Int4OID,
		"integer":   pgtype.Int4OID,
		"bigint":    pgtype.Int8OID,
		"real":      pgtype.Float4OID,
		"double":    pgtype.Float8OID,
		"date":      pgtype.DateOID,
		"time":      pgtype.TimeOID,
		"timestamp": pgtype.TimestampOID,
	}

	placeholders = regexp.MustCompile(`\$[0-9]+`)
)

const (
	timeout = 1 * time.Minute
)

func NewBackend(conn net.Conn, dsn string) (*Backend, error) {
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)

	err := trino.RegisterCustomClient("uncompressed", &http.Client{Transport: &http.Transport{DisableCompression: true}})
	if err != nil {
		return nil, err
	}

	// TODO parse the dns into an URL and rebuild it with the auth obtained from the pg conn
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}

	// TODO don't use a timeout here but allow canceling the context when receiving a terminate (or close) message
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	connHandler := &Backend{
		backend:   backend,
		conn:      conn,
		connInfo:  pgtype.NewConnInfo(),
		ctx:       ctx,
		ctxCancel: cancel,
		db:        db,
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
			err = p.closeStmt()
			if err != nil {
				break
			}
			err = p.handleQuery(msg)
		case *pgproto3.Parse:
			err = p.handlePrepare(msg)
		case *pgproto3.Bind:
			err = p.handleBind(msg)
		case *pgproto3.Describe:
			err = p.handleDescribe(msg)
		case *pgproto3.Execute:
			err = p.handleExecute(msg)
		case *pgproto3.Sync:
			status := byte('I')
			if p.stmt != nil {
				status = byte('T')
			}
			err = p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
			if err != nil {
				return fmt.Errorf("error writing sync response: %w", err)
			}
		case *pgproto3.Terminate:
			// this is a graceful request to close the connection, no cleanup is necessary here
			return nil
		case *pgproto3.FunctionCall:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: FunctionCall", NotImplementedErr))
		case *pgproto3.CancelRequest:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: CancelRequest", NotImplementedErr))
		case *pgproto3.Close:
			err = p.closeStmt()
			if err == nil {
				err = p.backend.Send(&pgproto3.CloseComplete{})
			}
		case *pgproto3.Flush:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: Flush", NotImplementedErr))
		case *pgproto3.CopyData:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: CopyData", NotImplementedErr))
		case *pgproto3.CopyDone:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: CopyDone", NotImplementedErr))
		case *pgproto3.CopyFail:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: CopyFail", NotImplementedErr))
		case *pgproto3.PasswordMessage:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: PasswordMessage", NotImplementedErr))
		case *pgproto3.GSSEncRequest:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: GSSEncRequest", NotImplementedErr))
		case *pgproto3.GSSResponse:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: GSSResponse", NotImplementedErr))
		case *pgproto3.SASLInitialResponse:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: SASLInitialResponse", NotImplementedErr))
		case *pgproto3.SASLResponse:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: SASLResponse", NotImplementedErr))
		default:
			err = newSoftError(pgerrcode.InternalError, fmt.Errorf("%w: %s", NotImplementedErr, msg))
		}
		if err != nil {
			var e *SoftError
			if !errors.As(err, &e) {
				return err
			}
			err = p.handleError(e)
			if err != nil {
				return err
			}
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
	// TODO this needs to handle multiple queries separate by ;
	query := strings.TrimRight(msg.String, ";")
	log.Println("INFO: Forwarding query", query)
	rows, err := p.db.QueryContext(p.ctx, query)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	desc, err := buildRowDescription(columns)
	if err != nil {
		return fmt.Errorf("error building row description: %w", err)
	}
	err = p.backend.Send(desc)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	err = p.writeRows(rows)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	err = p.backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}
	return nil
}

func (p *Backend) handlePrepare(msg *pgproto3.Parse) error {
	err := p.closeStmt()
	if err != nil {
		return err
	}
	if msg.Name != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named prepared statements are not supported"))
	}

	// TODO actually parse the query and avoid replacing literal values and quoted identifiers
	query := placeholders.ReplaceAllString(strings.TrimRight(msg.Query, ";"), "?")
	log.Println("INFO: Preparing query", query)
	p.stmtQuery = query
	p.stmt, err = p.db.PrepareContext(p.ctx, query)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}
	p.stmtTypes = msg.ParameterOIDs
	buf := (&pgproto3.ParseComplete{}).Encode(nil)
	_, err = p.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("error writing prepare response: %w", err)
	}
	return nil
}

func (p *Backend) closeStmt() error {
	if p.stmt == nil {
		return nil
	}
	err := p.stmt.Close()
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}
	p.stmtQuery = ""
	p.stmt = nil
	p.stmtTypes = nil
	p.stmtParams = nil
	if p.stmtRows != nil {
		err := p.stmtRows.Close()
		if err != nil {
			return newSoftError(pgerrcode.ConnectionException, err)
		}
	}
	p.stmtRows = nil
	return nil
}

func (p *Backend) handleBind(msg *pgproto3.Bind) error {
	if msg.DestinationPortal != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named destination portals are not supported"))
	}
	if msg.PreparedStatement != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named prepared statements are not supported"))
	}
	p.stmtParams = make([]interface{}, len(p.stmtTypes))
	for i, rawVal := range msg.Parameters {
		// TODO use connInfo.DataTypeForOID.Value.Get() ?
		var val interface{}
		switch pgtype.OID(p.stmtTypes[i]) {
		case pgtype.VarcharOID:
			val = new(string)
		case pgtype.Int2OID:
			val = new(int16)
		case pgtype.Int4OID:
			val = new(int32)
		case pgtype.Int8OID:
			val = new(int64)
		case pgtype.Float4OID:
			val = new(float32)
		case pgtype.Float8OID:
			val = new(float64)
		default:
			return newSoftError(pgerrcode.InvalidParameterValue, fmt.Errorf("Unknown parameter type: %+v", p.stmtTypes[i]))
		}
		err := p.connInfo.Scan(p.stmtTypes[i], msg.ParameterFormatCodes[i], rawVal, val)
		if err != nil {
			return newSoftError(pgerrcode.InvalidParameterValue, err)
		}
		// val is a pointer that would not be valid outside of this loop, need to copy the value
		switch pgtype.OID(p.stmtTypes[i]) {
		case pgtype.VarcharOID:
			p.stmtParams[i] = *(val.(*string))
		case pgtype.Int2OID:
			p.stmtParams[i] = *(val.(*int16))
		case pgtype.Int4OID:
			p.stmtParams[i] = *(val.(*int32))
		case pgtype.Int8OID:
			p.stmtParams[i] = *(val.(*int64))
		case pgtype.Float4OID:
			p.stmtParams[i] = *(val.(*float32))
		case pgtype.Float8OID:
			p.stmtParams[i] = *(val.(*float64))
		}
	}
	return p.backend.Send(&pgproto3.BindComplete{})
}

func (p *Backend) handleDescribe(msg *pgproto3.Describe) error {
	if msg.ObjectType == 'S' {
		return p.handleDescribeStatement(msg)
	}
	if msg.ObjectType == 'P' {
		return p.handleDescribePortal(msg)
	}
	return newSoftError(pgerrcode.ProtocolViolation, errors.New("invalid object type in describe message"))
}

func (p *Backend) handleDescribeStatement(msg *pgproto3.Describe) error {
	if msg.Name != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named prepared statements are not supported"))
	}

	// make sure a single session is being used
	conn, err := p.db.Conn(p.ctx)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	err = p.describeInput(conn)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	err = p.describeOutput(conn)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}

	return nil
}

func (p *Backend) describeInput(conn *sql.Conn) error {
	inputRows, err := conn.QueryContext(p.ctx, "DESCRIBE INPUT _trino_pg_gw", sql.Named("X-Trino-Prepared-Statement", "_trino_pg_gw="+url.QueryEscape(p.stmtQuery)))
	if err != nil {
		return err
	}
	defer inputRows.Close()

	params := make([]uint32, 0)
	var pos int
	var typeName string
	for inputRows.Next() {
		if err := inputRows.Scan(&pos, &typeName); err != nil {
			return err
		}
		params = append(params, trinoToPg[typeName])

	}
	if err := inputRows.Err(); err != nil {
		return err
	}
	if p.stmtTypes == nil {
		p.stmtTypes = params
	}
	return p.backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: params})
}

func (p *Backend) describeOutput(conn *sql.Conn) error {
	outputRows, err := conn.QueryContext(p.ctx, "DESCRIBE OUTPUT _trino_pg_gw", sql.Named("X-Trino-Prepared-Statement", "_trino_pg_gw="+url.QueryEscape(p.stmtQuery)))
	if err != nil {
		return err
	}
	defer outputRows.Close()

	fields := make([]pgproto3.FieldDescription, 0)
	var name string
	var catalog string
	var schema string
	var table string
	var typeName string
	var typeSize int
	var aliased bool
	for outputRows.Next() {
		if err := outputRows.Scan(&name, &catalog, &schema, &table, &typeName, &typeSize, &aliased); err != nil {
			return err
		}
		fields = append(fields, pgproto3.FieldDescription{
			Name:                 []byte(name),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          trinoToPg[typeName],
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	if err := outputRows.Err(); err != nil {
		return err
	}

	return p.backend.Send(&pgproto3.RowDescription{Fields: fields})
}

func (p *Backend) handleDescribePortal(msg *pgproto3.Describe) error {
	if msg.Name != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named portals are not supported"))
	}

	rows, err := p.getStmtRows()
	if err != nil {
		return err
	}

	columns, err := rows.ColumnTypes()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p.backend.Send(&pgproto3.NoData{})
		}
		return fmt.Errorf("error getting Trino query response columns: %w", err)
	}

	desc, err := buildRowDescription(columns)
	if err != nil {
		return fmt.Errorf("error building row description: %w", err)
	}

	return p.backend.Send(desc)
}

func (p *Backend) handleExecute(msg *pgproto3.Execute) error {
	if msg.Portal != "" {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("named destination portals are not supported"))
	}
	if msg.MaxRows != 0 {
		return newSoftError(pgerrcode.InvalidSQLStatementName, errors.New("max rows is not supported"))
	}

	rows, err := p.getStmtRows()
	if err != nil {
		return err
	}
	defer rows.Close()

	err = p.writeRows(rows)
	if err != nil {
		return newSoftError(pgerrcode.ConnectionException, err)
	}
	return nil
}

func (p *Backend) getStmtRows() (*sql.Rows, error) {
	if p.stmtRows != nil {
		return p.stmtRows, nil
	}

	log.Println("INFO: Forwarding prepared query")
	var err error
	p.stmtRows, err = p.stmt.QueryContext(p.ctx, p.stmtParams...)
	if err != nil {
		return p.stmtRows, newSoftError(pgerrcode.ConnectionException, err)
	}
	return p.stmtRows, nil
}

func (p *Backend) writeRows(rows *sql.Rows) error {
	columns, err := rows.ColumnTypes()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return p.backend.Send(&pgproto3.NoData{})
		}
		return fmt.Errorf("error getting Trino query response columns: %w", err)
	}

	// TODO build this from columns
	vals := make([]interface{}, len(columns))
	for i := range columns {
		vals[i] = new(interface{})
	}

	buf := []byte{}
	numRows := 0
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return fmt.Errorf("error scanning a Trino query response row: %w", err)
		}

		dataRow, err := p.buildDataRow(vals, columns)
		if err != nil {
			return fmt.Errorf("error building data row: %w", err)
		}
		buf = dataRow.Encode(buf)
		numRows++
	}
	rerr := rows.Close()
	if rerr != nil {
		return fmt.Errorf("error closing rows: %w", rerr)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error processing rows: %w", err)
	}

	_, err = p.conn.Write(buf)
	if err != nil {
		return fmt.Errorf("error writing query response: %w", err)
	}
	err = p.backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", numRows))})
	if err != nil {
		return err
	}
	return nil
}

func (p *Backend) handleError(err *SoftError) error {
	log.Println("ERROR:", err)
	// TODO decode Trino errors into fields
	werr := p.backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     err.Code,
		Message:  err.Error(),
	})
	if werr != nil {
		return fmt.Errorf("error writing error response: %w", werr)
	}
	return nil
}

func buildRowDescription(columns []*sql.ColumnType) (*pgproto3.RowDescription, error) {
	fields := make([]pgproto3.FieldDescription, len(columns))
	for i, column := range columns {
		var oid pgtype.OID
		var size int16 = -1
		s := column.ScanType()
		switch s {
		case reflect.TypeOf(sql.NullString{}):
			oid = pgtype.VarcharOID
		case reflect.TypeOf(sql.NullInt16{}):
			oid = pgtype.Int2OID
			size = 2
		case reflect.TypeOf(sql.NullInt32{}):
			oid = pgtype.Int4OID
			size = 4
		case reflect.TypeOf(sql.NullInt64{}):
			oid = pgtype.Int8OID
			size = 8
		case reflect.TypeOf(sql.NullFloat64{}):
			oid = pgtype.Float8OID
			size = 8
		default:
			return nil, errors.Errorf("Unknown scan type %T", s)
		}
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(columns[i].Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          uint32(oid),
			DataTypeSize:         size,
			TypeModifier:         -1,
			Format:               pgproto3.TextFormat,
		}
	}

	return &pgproto3.RowDescription{Fields: fields}, nil
}

func (p *Backend) buildDataRow(values []interface{}, columns []*sql.ColumnType) (*pgproto3.DataRow, error) {
	dr := &pgproto3.DataRow{
		Values: make([][]byte, len(values)),
	}

	var err error
	for i, rawVal := range values {
		var backendVal pgtype.ValueTranscoder
		ptr, ok := rawVal.(*interface{})
		if !ok {
			return nil, errors.Errorf("values[%d] (%+v) expected to be a pointer", i, columns[i])
		}
		status := pgtype.Present
		if *ptr == nil {
			status = pgtype.Null
		}
		switch v := (*ptr).(type) {
		case string:
			backendVal = &pgtype.Text{String: v, Status: status}
		case int16:
			backendVal = &pgtype.Int2{Int: v, Status: status}
		case int32:
			backendVal = &pgtype.Int4{Int: v, Status: status}
		case int64:
			backendVal = &pgtype.Int8{Int: v, Status: status}
		case float32:
			backendVal = &pgtype.Float4{Float: v, Status: status}
		case float64:
			backendVal = &pgtype.Float8{Float: v, Status: status}
		default:
			return nil, errors.Errorf("Unknown scan type %T", v)
		}

		dr.Values[i], err = backendVal.EncodeText(p.connInfo, nil)
		if err != nil {
			err = errors.Errorf("failed to encode values[%d] (%+v)", i, columns[i])
			return nil, newSoftError(pgerrcode.InvalidParameterValue, err)
		}
	}

	return dr, nil
}

func (p *Backend) Close() error {
	dbErr := p.db.Close()
	connErr := p.conn.Close()
	p.ctxCancel()
	if dbErr != nil {
		return dbErr
	}
	return connErr
}
