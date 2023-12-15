package bigquery

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
)

// Can't keep this test long term unless have access to bigquery for testing
func TestCanWrite(t *testing.T) {
	// I want to test
	// - can write all the types.
	// - flat integers get written as the correct value
	// - can write nested structs
	// - can write slices of structs
	// - zero values (which are omitted from the plenc) are written as zero, not null
	// - the null and point to scalar types are handled correctly - including null written as null
	// - can write time.Time as timestamps
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

	type WithNested struct {
		A SimpleStruct    `plenc:"1"`
		B []SimpleStruct  `plenc:"2"`
		C map[string]bool `plenc:"3"`
	}

	tests := []struct {
		name string
		in   []any
	}{}

	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cli, err := managedwriter.NewClient(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	bq := New(cli, log)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec, err := plenc.CodecForType(reflect.TypeOf(tt.in[0]))
			if err != nil {
				t.Fatal(err)
			}
			d := codec.Descriptor()

			up, err := bq.MakeUploader(ctx, &protocol.ConnectionDescriptor{
				ProjectID:  "philpearl",
				DataSetID:  "test",
				TableName:  "test",
				Descriptor: d,
			})
			if err != nil {
				t.Fatal(err)
			}

			var data []byte
			for _, v := range tt.in {
				var err error
				data, err = plenc.Marshal(data[:0], v)
				if err != nil {
					t.Fatal(err)
				}
				d := up.BufferFor(len(data))
				copy(d, data)
				up.Add(d)
			}

			upl := up.(*upload)
			upl.Wait()
		})
	}
}
