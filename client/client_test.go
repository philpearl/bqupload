package client_test

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/bqupload/client"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/bqupload/server"
	"github.com/philpearl/plenc"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"
)

type fakeUploader struct {
	mu   sync.Mutex
	data [][]byte
}

func (f *fakeUploader) BufferFor(size int) []byte {
	return make([]byte, size)
}

// add is called to note that the buffer returned by bufferFor was used and
// is full of useful data,
func (f *fakeUploader) Add(buf []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = append(f.data, buf)
}

func (f *fakeUploader) Flush()    {}
func (f *fakeUploader) TryFlush() {}

func TestClient(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	var pl plenc.Plenc
	pl.RegisterDefaultCodecs()

	type thing struct {
		A int    `plenc:"1"`
		B string `plenc:"2"`
	}

	codec, err := pl.CodecForType(reflect.TypeOf(thing{}))
	if err != nil {
		t.Fatal(err)
	}

	desc := &protocol.ConnectionDescriptor{
		ProjectID:  "test",
		DataSetID:  "test",
		TableName:  "test",
		Descriptor: codec.Descriptor(),
	}

	var fu fakeUploader
	factory := func(ctx context.Context, desc *protocol.ConnectionDescriptor) (bigquery.Uploader, error) {
		return &fu, err
	}

	s, err := server.New("localhost:0", log, noop.Meter{}, factory)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("starting server: %s", err)
	}

	defer func() {
		if err := s.Stop(); err != nil {
			t.Errorf("stopping server: %s", err)
		}
	}()

	cli, err := client.New(s.Addr().String(), &pl, desc)
	if err != nil {
		t.Fatalf("creating client: %s", err)
	}

	v := thing{A: 1, B: "two"}

	var eg errgroup.Group
	for j := 0; j < 10; j++ {
		eg.Go(func() error {
			for i := 0; i < 100; i++ {
				if err := cli.Publish(ctx, &v); err != nil {
					t.Fatalf("publishing: %s", err)
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("waiting for publish: %s", err)
	}

	if len(fu.data) != 1000 {
		t.Errorf("wrong number of buffers: %d", len(fu.data))
	}

	for i, d := range fu.data {
		var got thing
		if err := pl.Unmarshal(d, &got); err != nil {
			t.Errorf("unmarshalling buffer %d: %s", i, err)
		}
		if got != v {
			t.Errorf("buffer %d: got %v, want %v", i, got, v)
		}
	}
}
