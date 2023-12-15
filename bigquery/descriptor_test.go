package bigquery_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/philpearl/bqupload/bigquery"
	"github.com/philpearl/plenc"
	"github.com/philpearl/plenc/plenccodec"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestDescriptor(t *testing.T) {
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
		name  string
		in    any
		plenc plenccodec.Descriptor
		want  *descriptorpb.DescriptorProto
	}{
		{
			name: "simple struct",
			in:   SimpleStruct{},
			plenc: plenccodec.Descriptor{
				TypeName: "SimpleStruct",
				Type:     plenccodec.FieldTypeStruct,
				Elements: []plenccodec.Descriptor{
					{Name: "A", Index: 1, Type: plenccodec.FieldTypeInt},
					{Name: "B", Index: 2, Type: plenccodec.FieldTypeUint},
					{Name: "C", Index: 3, Type: plenccodec.FieldTypeString},
				},
			},
			want: &descriptorpb.DescriptorProto{
				Name: proto.String("SimpleStruct"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:         proto.String("A"),
						Number:       proto.Int32(1),
						Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:         descriptorpb.FieldDescriptorProto_TYPE_SINT64.Enum(),
						DefaultValue: proto.String("0"),
					},
					{
						Name:         proto.String("B"),
						Number:       proto.Int32(2),
						Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:         descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
						DefaultValue: proto.String("0"),
					},
					{
						Name:         proto.String("C"),
						Number:       proto.Int32(3),
						Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:         descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						DefaultValue: proto.String(""),
					},
				},
			},
		},
		{
			name: "nested struct",
			in:   WithNested{},
			plenc: plenccodec.Descriptor{
				TypeName: "WithNested",
				Type:     plenccodec.FieldTypeStruct,
				Elements: []plenccodec.Descriptor{
					{
						Name:     "A",
						Index:    1,
						Type:     plenccodec.FieldTypeStruct,
						TypeName: "SimpleStruct",
						Elements: []plenccodec.Descriptor{
							{Name: "A", Index: 1, Type: plenccodec.FieldTypeInt},
							{Name: "B", Index: 2, Type: plenccodec.FieldTypeUint},
							{Name: "C", Index: 3, Type: plenccodec.FieldTypeString},
						},
					},
					{
						Name:  "B",
						Index: 2,
						Type:  plenccodec.FieldTypeSlice,
						Elements: []plenccodec.Descriptor{
							{
								TypeName: "SimpleStruct",
								Type:     plenccodec.FieldTypeStruct,
								Elements: []plenccodec.Descriptor{
									{Name: "A", Index: 1, Type: plenccodec.FieldTypeInt},
									{Name: "B", Index: 2, Type: plenccodec.FieldTypeUint},
									{Name: "C", Index: 3, Type: plenccodec.FieldTypeString},
								},
							},
						},
					},
					{
						Name:  "C",
						Index: 3,
						Type:  plenccodec.FieldTypeSlice,
						Elements: []plenccodec.Descriptor{
							{
								Type:     plenccodec.FieldTypeStruct,
								TypeName: "map_FieldTypeString_FieldTypeBool",
								Elements: []plenccodec.Descriptor{
									{Name: "key", Index: 1, Type: plenccodec.FieldTypeString},
									{Name: "value", Index: 2, Type: plenccodec.FieldTypeBool},
								},
							},
						},
					},
				},
			},
			want: &descriptorpb.DescriptorProto{
				Name: proto.String("WithNested"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("A"),
						Number:   proto.Int32(1),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String("SimpleStruct"),
					},
					{
						Name:     proto.String("B"),
						Number:   proto.Int32(2),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String("SimpleStruct"),
					},
					{
						Name:     proto.String("C"),
						Number:   proto.Int32(3),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String("map_FieldTypeString_FieldTypeBool"),
					},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("SimpleStruct"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:         proto.String("A"),
								Number:       proto.Int32(1),
								Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:         descriptorpb.FieldDescriptorProto_TYPE_SINT64.Enum(),
								DefaultValue: proto.String("0"),
							},
							{
								Name:         proto.String("B"),
								Number:       proto.Int32(2),
								Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:         descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
								DefaultValue: proto.String("0"),
							},
							{
								Name:         proto.String("C"),
								Number:       proto.Int32(3),
								Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:         descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								DefaultValue: proto.String(""),
							},
						},
					},
					{
						Name: proto.String("map_FieldTypeString_FieldTypeBool"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:         proto.String("key"),
								Number:       proto.Int32(1),
								Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:         descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
								DefaultValue: proto.String(""),
							},
							{
								Name:         proto.String("value"),
								Number:       proto.Int32(2),
								Label:        descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
								Type:         descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
								DefaultValue: proto.String("false"),
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codec, err := plenc.CodecForType(reflect.TypeOf(tt.in))
			if err != nil {
				t.Fatal(err)
			}

			desc := codec.Descriptor()

			if diff := cmp.Diff(tt.plenc, desc); diff != "" {
				t.Errorf("plenc descriptor mismatch (-want +got):\n%s", diff)
			}

			got, err := bigquery.PlencDescriptorToProtobuf(&desc)
			if err != nil {
				t.Errorf("plencDescriptorToProtobuf() error = %v", err)
				return
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreUnexported(descriptorpb.DescriptorProto{}, descriptorpb.FieldDescriptorProto{})); diff != "" {
				t.Errorf("proto descriptor mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
