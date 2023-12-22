package bigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/google/go-cmp/cmp"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
	plencnull "github.com/philpearl/plenc/null"
	"github.com/philpearl/plenc/plenccodec"
	"github.com/unravelin/null"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Can't keep this test long term unless have access to bigquery for testing
func TestCanWrite(t *testing.T) {
	projectID := os.Getenv("TEST_BQ_PROJECT")
	datasetID := os.Getenv("TEST_BQ_DATASET")
	tableID := os.Getenv("TEST_BQ_TABLE")

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := createTestTable(client, projectID, datasetID, tableID); err != nil {
		t.Fatal(err)
	}

	// I want to test
	// - [x] can write all the types.
	// - [x] flat integers get written as the correct value
	// - [x] can write nested structs
	// - [x] can write slices of structs
	// - [x] zero values (which are omitted from the plenc) are written as zero, not null
	// - [x] the null and point to scalar types are handled correctly - including null written as null
	// - [x] can write time.Time as timestamps
	// - [x] what happens if I add a field to the struct that's not in the schema (get an error!)
	//
	// What should I do to test this?
	// - want a single big struct that can test all of this. All the types
	// - create a table with a matching schema
	// - use env variables to control the project, dataset and table names
	// - write a bunch of rows that test all the cases
	// - read the rows back and check they match
	type SimpleStruct struct {
		A int    `plenc:"1"`
		B uint32 `plenc:"2"`
		C string `plenc:"3"`
	}

	type Row struct {
		A SimpleStruct   `plenc:"1"`
		B []SimpleStruct `plenc:"2"`
		// ideally we wouldn't need this proto tag and would be able to pick
		// proto-compatible maps just when sending to BQ.
		// Also maps aren't really supported by big query.
		// C map[string]bool `plenc:"3,proto"`
		D int         `plenc:"4"`
		E uint32      `plenc:"5"`
		F string      `plenc:"6"`
		G []int       `plenc:"7"`
		H int         `plenc:"8,flat"`
		I float32     `plenc:"9"`
		J float64     `plenc:"10"`
		K time.Time   `plenc:"11"`
		M bool        `plenc:"13"`
		N null.Int    `plenc:"14"`
		O null.Float  `plenc:"15"`
		P null.Bool   `plenc:"16"`
		Q null.String `plenc:"17"`
	}
	var p plenc.Plenc
	p.ProtoCompatibleArrays = true
	p.RegisterDefaultCodecs()
	plencnull.AddCodecs(&p)
	p.RegisterCodec(reflect.TypeOf(time.Time{}), plenccodec.BQTimestampCodec{})

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cli, err := managedwriter.NewClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()

	bq := New(cli, log, noop.Meter{}, dir)

	rows := []Row{
		{
			A: SimpleStruct{A: 1, B: 2, C: "three"},
			B: []SimpleStruct{
				{A: 4, B: 5, C: "six"},
				{A: -1337, B: 0, C: ""},
			},
			// C: map[string]bool{
			// 	"one": true,
			// 	"two": false,
			// },
			D: 8,
			E: 9,
			F: "ten",
			G: []int{11, 12, 13},
			H: 14010102023,
			I: 15.16,
			J: 17.18,
			K: time.Date(2019, 12, 31, 23, 59, 59, 999999999, time.UTC),
			M: true,
			N: null.I(32),
			O: null.F(24.36),
			P: null.B(true),
			Q: null.S("twenty six"),
		},
		{},
	}

	codec, err := p.CodecForType(reflect.TypeOf(rows[0]))
	if err != nil {
		t.Fatal(err)
	}
	d := codec.Descriptor()

	up, err := bq.GetUploader(ctx, &protocol.ConnectionDescriptor{
		ProjectID:  projectID,
		DataSetID:  datasetID,
		TableName:  tableID,
		Descriptor: d,
	})
	if err != nil {
		t.Fatal(err)
	}

	var data []byte
	for i := range rows {
		v := &rows[i]
		var err error
		data, err = p.Marshal(data[:0], v)
		if err != nil {
			t.Fatal(err)
		}
		d := up.BufferFor(len(data))
		copy(d, data)
		up.Add(d)
	}

	// This ensures the data is written to the tablewriter
	up.Flush()

	// But we have then to wait until the table writers have drained and shut down
	bq.Wait()

	it, err := client.Query(fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)).Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var got []Row
	for {
		var r Row

		if err := it.Next(&r); err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}

	if diff := cmp.Diff(rows, got); diff != "" {
		t.Fatal(diff)
	}

	// TODO:
	// - [x] clean table before starting
	// - [ ] check the rows are there and correct
	// - [ ] table reading method doesn't work with null.XXX types.
}

func createTestTable(client *bigquery.Client, projectID, datasetID, tableID string) error {
	ctx := context.Background()

	schema := bigquery.Schema{
		{Name: "A", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.IntegerFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
			{Name: "C", Type: bigquery.StringFieldType},
		}},
		{Name: "B", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "A", Type: bigquery.IntegerFieldType},
			{Name: "B", Type: bigquery.IntegerFieldType},
			{Name: "C", Type: bigquery.StringFieldType},
		}, Repeated: true},
		// {Name: "C", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
		// 	{Name: "key", Type: bigquery.StringFieldType},
		// 	{Name: "value", Type: bigquery.BooleanFieldType},
		// }, Repeated: true},
		{Name: "D", Type: bigquery.IntegerFieldType},
		{Name: "E", Type: bigquery.IntegerFieldType},
		{Name: "F", Type: bigquery.StringFieldType},
		{Name: "G", Type: bigquery.IntegerFieldType, Repeated: true},
		{Name: "H", Type: bigquery.IntegerFieldType},
		{Name: "I", Type: bigquery.FloatFieldType},
		{Name: "J", Type: bigquery.FloatFieldType},
		{Name: "K", Type: bigquery.TimestampFieldType},
		{Name: "L", Type: bigquery.IntegerFieldType},
		{Name: "M", Type: bigquery.BooleanFieldType},
		{Name: "N", Type: bigquery.IntegerFieldType},
		{Name: "O", Type: bigquery.FloatFieldType},
		{Name: "P", Type: bigquery.BooleanFieldType},
		{Name: "Q", Type: bigquery.StringFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         schema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)

	err := tableRef.Create(ctx, metaData)
	if err == nil {
		// Table created successfully.
		return nil
	}
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) || gerr.Code != 409 {
		return fmt.Errorf("creating table: %w", err)
	}

	// table already exists. Is the schema the same?
	tmd, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}

	newMeta := bigquery.TableMetadataToUpdate{
		Schema: schema,
	}
	if _, err := tableRef.Update(ctx, newMeta, tmd.ETag); err != nil {
		return err
	}

	j, err := client.Query(fmt.Sprintf("DELETE FROM `%s.%s.%s` WHERE 1=1", projectID, datasetID, tableID)).Run(ctx)
	if err != nil {
		return fmt.Errorf("clearing table: %w", err)
	}

	if _, err := j.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for clear: %w", err)
	}
	return nil
}
