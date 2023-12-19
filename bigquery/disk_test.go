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

	drm := NewDiskReadManager(dir, log, func(ctx context.Context, cd *protocol.ConnectionDescriptor) (chan<- uploadBuffer, error) {
		return ch, nil
	})
	drm.Start(ctx)

	hashb := sha256.Sum256(encodedDesc)
	hash := hex.EncodeToString(hashb[:])

	dw, err := newDiskWriter(filepath.Join(dir, desc.ProjectID, desc.DataSetID, desc.TableName, hash), log, encodedDesc)
	if err != nil {
		t.Fatal(err)
	}
	dw.start()

	// write some data to the disk writer
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:3], buf[3:]}, buf: buf}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:4], buf[4:]}, buf: buf}
	dw.in <- uploadBuffer{Data: [][]byte{buf[:5], buf[5:]}, buf: buf}

	var result []uploadBuffer
	for i := 0; i < 3; i++ {
		result = append(result, <-ch)
	}

	// TODO: order is random because of random uuids
	if diff := cmp.Diff([]uploadBuffer{
		{Data: [][]byte{buf[:3], buf[3:]}, buf: buf},
		{Data: [][]byte{buf[:4], buf[4:]}, buf: buf},
		{Data: [][]byte{buf[:5], buf[5:]}, buf: buf},
	}, result, cmpopts.IgnoreUnexported(uploadBuffer{})); diff != "" {
		t.Fatal(diff)
	}
}
