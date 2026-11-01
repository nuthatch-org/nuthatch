// The Flight SQL client contract (RFC-0079 §16): Apache Arrow ADBC's Flight SQL driver, the Go
// implementation every ADBC binding loads, against `nuthatch dev --flight` over the runtime that
// tests/e2e_flight_clients.rs builds. It shares no code with nuthatch's own arrow-flight crate.
//
// Usage: flight-client -flight 127.0.0.1:8815 -http http://127.0.0.1:8288 -token t
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const statement = `SELECT block_number, log_index, tx_hash, "from", "to", value, value_dec, value_overflow
FROM tab__transfer ORDER BY block_number, log_index`

var (
	flightAddr = flag.String("flight", "", "the Flight SQL address, host:port")
	httpBase   = flag.String("http", "", "the HTTP API, for the JSON answer to compare with")
	token      = flag.String("token", "", "a bearer token every call carries")
	failed     = false
)

func check(name string, err error) {
	if err != nil {
		failed = true
		fmt.Printf("FAIL %s: %v\n", name, err)
		return
	}
	fmt.Printf("ok   %s\n", name)
}

// connect opens a connection whose every call names `route` in x-nuthatch-route ("" for none).
func connect(ctx context.Context, route string) (adbc.Connection, error) {
	opts := map[string]string{adbc.OptionKeyURI: "grpc://" + *flightAddr}
	if *token != "" {
		opts[flightsql.OptionAuthorizationHeader] = "Bearer " + *token
	}
	if route != "" {
		opts[flightsql.OptionRPCCallHeaderPrefix+"x-nuthatch-route"] = route
	}
	db, err := flightsql.NewDriver(memory.DefaultAllocator).NewDatabase(opts)
	if err != nil {
		return nil, err
	}
	return db.Open(ctx)
}

func query(ctx context.Context, cnxn adbc.Connection, sql string) ([]string, [][]*string, error) {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()
	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, nil, err
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer rdr.Release()
	var names []string
	for _, f := range rdr.Schema().Fields() {
		names = append(names, f.Name)
	}
	var rows [][]*string
	for rdr.Next() {
		rec := rdr.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			row := make([]*string, rec.NumCols())
			for c, col := range rec.Columns() {
				if col.IsValid(i) {
					v := col.ValueStr(i)
					row[c] = &v
				}
			}
			rows = append(rows, row)
		}
	}
	return names, rows, rdr.Err()
}

// sqlJSON is POST /<route>/sql, the JSON answer the Flight one must equal.
func sqlJSON(route, sql string) (columns []string, rows [][]*string, err error) {
	body, _ := json.Marshal(map[string]string{"q": sql})
	resp, err := http.Post(*httpBase+"/"+route+"/sql", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var answer struct {
		Columns   []string                     `json:"columns"`
		Rows      []map[string]json.RawMessage `json:"rows"`
		Truncated bool                         `json:"truncated"`
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("/sql answered %d: %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, nil, err
	}
	if answer.Truncated {
		return nil, nil, errors.New("/sql truncated the answer, so it cannot be compared whole")
	}
	for _, r := range answer.Rows {
		row := make([]*string, len(answer.Columns))
		for c, name := range answer.Columns {
			row[c] = scalar(r[name])
		}
		rows = append(rows, row)
	}
	return answer.Columns, rows, nil
}

// scalar is a JSON value as text: a number's own digits, a string unquoted, null as nil.
func scalar(raw json.RawMessage) *string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if dec.Decode(&v) != nil || v == nil {
		return nil
	}
	s := fmt.Sprint(v)
	return &s
}

func show(v *string) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%q", *v)
}

func sameAnswer(ctx context.Context, cnxn adbc.Connection, route, sql string) error {
	names, rows, err := query(ctx, cnxn, sql)
	if err != nil {
		return err
	}
	columns, want, err := sqlJSON(route, sql)
	if err != nil {
		return err
	}
	if !slices.Equal(names, columns) {
		return fmt.Errorf("columns %v, /sql has %v", names, columns)
	}
	if len(want) == 0 {
		return errors.New("premise: /sql answered no rows, so nothing was compared")
	}
	if len(rows) != len(want) {
		return fmt.Errorf("%d rows, /sql has %d", len(rows), len(want))
	}
	for i := range want {
		for c := range columns {
			a, b := rows[i][c], want[i][c]
			if (a == nil) != (b == nil) || (a != nil && *a != *b) {
				return fmt.Errorf("row %d %s: Flight %s, /sql %s", i, columns[c], show(a), show(b))
			}
		}
	}
	fmt.Printf("     %d rows x %d columns equal\n", len(want), len(columns))
	return nil
}

func one(ctx context.Context, cnxn adbc.Connection, sql string) (string, error) {
	_, rows, err := query(ctx, cnxn, sql)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 || rows[0][0] == nil {
		return "", fmt.Errorf("want one value, got %d rows", len(rows))
	}
	return *rows[0][0], nil
}

// refused requires an ADBC error with `code` whose message carries /sql's JSON error and `needle`.
func refused(err error, code adbc.Status, grpc, needle string) error {
	var e adbc.Error
	if err == nil {
		return errors.New("answered, not refused")
	}
	if !errors.As(err, &e) {
		return fmt.Errorf("not an ADBC error: %v", err)
	}
	if e.Code != code || !strings.Contains(e.Msg, grpc) {
		return fmt.Errorf("status %v, want %v (%s): %s", e.Code, code, grpc, e.Msg)
	}
	if needle != "" && (!strings.Contains(e.Msg, `{"error"`) || !strings.Contains(e.Msg, needle)) {
		return fmt.Errorf("the message does not carry /sql's JSON error naming %q: %s", needle, e.Msg)
	}
	return nil
}

type catalog struct {
	Name    string `json:"catalog_name"`
	Schemas []struct {
		Name   string `json:"db_schema_name"`
		Tables []struct {
			Name    string `json:"table_name"`
			Type    string `json:"table_type"`
			Columns []struct {
				Name string `json:"column_name"`
			} `json:"table_columns"`
		} `json:"db_schema_tables"`
	} `json:"catalog_db_schemas"`
}

// tables is GetObjects at full depth: each table found, with its column names.
func tables(ctx context.Context, cnxn adbc.Connection) (map[string][]string, error) {
	rdr, err := cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	out := map[string][]string{}
	for rdr.Next() {
		var buf bytes.Buffer
		if err := array.RecordToJSON(rdr.RecordBatch(), &buf); err != nil {
			return nil, err
		}
		dec := json.NewDecoder(&buf)
		for dec.More() {
			var c catalog
			if err := dec.Decode(&c); err != nil {
				return nil, err
			}
			for _, s := range c.Schemas {
				for _, t := range s.Tables {
					var cols []string
					for _, col := range t.Columns {
						cols = append(cols, col.Name)
					}
					out[c.Name+"."+s.Name+"."+t.Name+" "+t.Type] = cols
				}
			}
		}
	}
	return out, rdr.Err()
}

func fieldNames(s *arrow.Schema) []string {
	var out []string
	for _, f := range s.Fields() {
		out = append(out, f.Name)
	}
	return out
}

func main() {
	flag.Parse()
	if *flightAddr == "" || *httpBase == "" {
		fmt.Fprintln(os.Stderr, "usage: flight-client -flight host:port -http http://host:port [-token t]")
		os.Exit(2)
	}
	ctx := context.Background()
	a, err := connect(ctx, "a")
	if err != nil {
		fmt.Printf("FAIL connect: %v\n", err)
		os.Exit(1)
	}
	defer a.Close()
	b, err := connect(ctx, "b")
	if err != nil {
		fmt.Printf("FAIL connect: %v\n", err)
		os.Exit(1)
	}
	defer b.Close()

	check("a statement's rows equal POST /a/sql's, value for value", sameAnswer(ctx, a, "a", statement))

	check("x-nuthatch-route picks the mount", func() error {
		for cnxn, want := range map[adbc.Connection]string{a: "a", b: "b"} {
			got, err := one(ctx, cnxn, "SELECT which FROM which_nest")
			if err != nil {
				return err
			}
			if got != want {
				return fmt.Errorf("route %s answered %q", want, got)
			}
		}
		return nil
	}())

	check("no route on a runtime is NotFound, as /sql at the root is 404", func() error {
		root, err := connect(ctx, "")
		if err != nil {
			return err
		}
		defer root.Close()
		_, _, err = query(ctx, root, "SELECT 1")
		return refused(err, adbc.StatusNotFound, "NotFound", "")
	}())

	check("a refusal is InvalidArgument carrying /sql's JSON error", func() error {
		_, _, err := query(ctx, a, "SELECT no_such_column FROM tab__transfer")
		return refused(err, adbc.StatusInvalidArgument, "InvalidArgument", "no_such_column")
	}())

	check("GetObjects lists the route's tables with the columns a statement returns", func() error {
		found, err := tables(ctx, a)
		if err != nil {
			return err
		}
		want, _, err := sqlJSON("a", "SELECT * FROM tab__transfer LIMIT 1")
		if err != nil {
			return err
		}
		got, ok := found["memory.main.tab__transfer VIEW"]
		if !ok {
			return fmt.Errorf("tab__transfer not listed: %v", found)
		}
		if !slices.Equal(got, want) {
			return fmt.Errorf("columns %v, /sql has %v", got, want)
		}
		if _, ok := found["memory.main.which_nest VIEW"]; !ok {
			return fmt.Errorf("the view which_nest not listed: %v", found)
		}
		fmt.Printf("     %d tables\n", len(found))
		return nil
	}())

	check("GetTableSchema and GetTableTypes answer", func() error {
		s, err := a.GetTableSchema(ctx, nil, nil, "which_nest")
		if err != nil {
			return err
		}
		if got := fieldNames(s); !slices.Equal(got, []string{"which"}) {
			return fmt.Errorf("which_nest has columns %v", got)
		}
		rdr, err := a.GetTableTypes(ctx)
		if err != nil {
			return err
		}
		defer rdr.Release()
		var kinds []string
		for rdr.Next() {
			col := rdr.RecordBatch().Column(0)
			for i := 0; i < col.Len(); i++ {
				kinds = append(kinds, col.ValueStr(i))
			}
		}
		if !slices.Contains(kinds, "VIEW") {
			return fmt.Errorf("table types %v", kinds)
		}
		return rdr.Err()
	}())

	if failed {
		os.Exit(1)
	}
}
