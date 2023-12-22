package bigquery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/philpearl/bqupload/protocol"
	"github.com/philpearl/plenc"
	"github.com/philpearl/plenc/plenccodec"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestDisk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()

	desc := protocol.ConnectionDescriptor{
		ProjectID: "projectID",
		DataSetID: "dataSetID",
		TableName: "tableName",
		Descriptor: plenccodec.Descriptor{
			TypeName: "SimpleStruct",
			Type:     plenccodec.FieldTypeStruct,
			Elements: []plenccodec.Descriptor{
				{Name: "A", Index: 1, Type: plenccodec.FieldTypeInt},
				{Name: "B", Index: 2, Type: plenccodec.FieldTypeUint},
				{Name: "C", Index: 3, Type: plenccodec.FieldTypeString},
			},
		},
	}

	encodedDesc, err := plenc.Marshal(nil, &desc.Descriptor)
	if err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ch := make(chan uploadBuffer)

	drm, err := NewDiskReadManager(dir, log, noop.Meter{}, func(ctx context.Context, cd *protocol.ConnectionDescriptor) (chan<- uploadBuffer, error) {
		return ch, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	drm.Start(ctx)

	hashb := sha256.Sum256(encodedDesc)
	hash := hex.EncodeToString(hashb[:])

	var dwm diskWriteMetrics
	if err := dwm.init(noop.Meter{}); err != nil {
		t.Fatal(err)
	}

	dw, err := newDiskWriter(filepath.Join(dir, desc.ProjectID, desc.DataSetID, desc.TableName, hash), log, dwm, *attribute.EmptySet(), encodedDesc)
	if err != nil {
		t.Fatal(err)
	}
	dw.start(ctx)

	// write some data to the disk writer
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:3], buf[3:]}, buf: buf}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:4], buf[4:]}, buf: buf}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:5], buf[5:]}, buf: buf}

	var result []uploadBuffer
	for i := 0; i < 3; i++ {
		u := <-ch
		u.f(u, nil)
		result = append(result, u)
	}

	// Random uuids mean we have to sort the results
	if diff := cmp.Diff(
		[]uploadBuffer{
			{Data: [][]byte{buf[:3], buf[3:]}, buf: buf},
			{Data: [][]byte{buf[:4], buf[4:]}, buf: buf},
			{Data: [][]byte{buf[:5], buf[5:]}, buf: buf},
		},
		result,
		cmpopts.IgnoreUnexported(uploadBuffer{}),
		cmpopts.SortSlices(func(a, b uploadBuffer) bool {
			return len(a.Data[0]) < len(b.Data[0])
		}),
	); diff != "" {
		t.Fatal(diff)
	}
}
