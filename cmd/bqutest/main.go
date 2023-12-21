package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/philpearl/bqupload/client"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
	plencnull "github.com/philpearl/plenc/null"
	"github.com/philpearl/plenc/plenccodec"
	"github.com/unravelin/null"
	"golang.org/x/sync/errgroup"
)

type opt struct {
	projectID string
	dataSetID string
	tableName string

	addr       string
	msgCount   int
	goRoutines int
}

func (o *opt) registerFlags() {
	flag.StringVar(&o.projectID, "project", "", "project ID")
	flag.StringVar(&o.dataSetID, "dataset", "", "dataset ID")
	flag.StringVar(&o.tableName, "table", "", "table ID")
	flag.StringVar(&o.addr, "addr", "localhost:8123", "address to connect to")
	flag.IntVar(&o.msgCount, "msg-count", 10, "number of messages to send")
	flag.IntVar(&o.goRoutines, "go-routines", 1, "number of go routines to use")
}

func (o *opt) validate() error {
	if o.projectID == "" {
		return fmt.Errorf("project is required")
	}
	if o.dataSetID == "" {
		return fmt.Errorf("dataset is required")
	}
	if o.tableName == "" {
		return fmt.Errorf("table is required")
	}
	return nil
}

func main() {
	var o opt
	o.registerFlags()
	flag.Parse()

	if err := o.run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

type TestMessage struct {
	Name      string    `json:"name,omitempty" plenc:"1"`
	Age       null.Int  `json:"age,omitempty" plenc:"2"`
	IsARabbit bool      `json:"isARabbit,omitempty" plenc:"3"`
	Time      time.Time `json:"time,omitempty" plenc:"4"`
}

func (o *opt) run() error {
	if err := o.validate(); err != nil {
		return fmt.Errorf("validating options: %w", err)
	}

	// Set up the plenc codec
	var pl plenc.Plenc
	pl.ProtoCompatibleArrays = true
	pl.RegisterDefaultCodecs()
	plencnull.AddCodecs(&pl)
	pl.RegisterCodec(reflect.TypeOf(time.Time{}), plenccodec.BQTimestampCodec{})

	codec, err := pl.CodecForType(reflect.TypeOf(TestMessage{}))
	if err != nil {
		return fmt.Errorf("getting codec: %w", err)
	}

	desc := protocol.ConnectionDescriptor{
		ProjectID:  o.projectID,
		DataSetID:  o.dataSetID,
		TableName:  o.tableName,
		Descriptor: codec.Descriptor(),
	}

	cli, err := client.New(o.addr, &pl, &desc)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	msg := TestMessage{
		Name:      "Phil",
		Age:       null.I(53),
		IsARabbit: false,
		Time:      time.Now(),
	}

	ctx := context.Background()

	var eg errgroup.Group

	start := time.Now()
	defer func() {
		fmt.Printf("Time taken: %s\n", time.Since(start))
	}()
	for i := 0; i < o.goRoutines; i++ {
		eg.Go(func() error {
			for i := 0; i < o.msgCount; i++ {
				if err := cli.Publish(ctx, msg); err != nil {
					return fmt.Errorf("publishing (%d): %w", i, err)
				}
			}
			return nil
		})
	}

	return eg.Wait()
}
