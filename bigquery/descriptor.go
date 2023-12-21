package bigquery

import (
	"fmt"

	"github.com/philpearl/plenc/plenccodec"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// - [ ] Descriptor should perhaps use named nested struct definitions

type descriptorBuilder struct {
	root descriptorpb.DescriptorProto
}

func PlencDescriptorToProtobuf(pd *plenccodec.Descriptor) (*descriptorpb.DescriptorProto, error) {
	var b descriptorBuilder
	err := b.handleStruct(pd, &b.root)

	return &b.root, err
}

func (b *descriptorBuilder) handleStruct(pd *plenccodec.Descriptor, pb *descriptorpb.DescriptorProto) error {
	if pd.Type != plenccodec.FieldTypeStruct {
		return fmt.Errorf("descriptor is not a struct - type is %s", pd.Type)
	}

	pb.Field = make([]*descriptorpb.FieldDescriptorProto, len(pd.Elements))
	for i, elt := range pd.Elements {
		var f descriptorpb.FieldDescriptorProto

		f.Name = proto.String(elt.Name)
		f.Number = proto.Int32(int32(elt.Index))
		f.Label = descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()

		if err := b.handleFieldType(&elt, &f); err != nil {
			return fmt.Errorf("handling field %d %s: %w", i, elt.Name, err)
		}
		pb.Field[i] = &f
	}

	pb.Name = proto.String(pd.TypeName)

	return nil
}

func (b *descriptorBuilder) handleFieldType(elt *plenccodec.Descriptor, f *descriptorpb.FieldDescriptorProto) error {
	var dtyp descriptorpb.FieldDescriptorProto_Type
	switch elt.Type {
	case plenccodec.FieldTypeInt:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_SINT64
	case plenccodec.FieldTypeUint:
		// BigQuery doesn't appear to support uint64. We'll use uint32, but we
		// probably should distinguish these cases
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_UINT32
	case plenccodec.FieldTypeFloat32:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_FLOAT
	case plenccodec.FieldTypeFloat64:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case plenccodec.FieldTypeString:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_STRING
	case plenccodec.FieldTypeSlice:
		// Note maps are encoded as slices of structs of key and value
		f.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		if len(elt.Elements) != 1 {
			return fmt.Errorf("slice type should have exactly one element")
		}

		// Most slices must be noted as packed encoding
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
		if err := b.handleFieldType(&elt.Elements[0], f); err != nil {
			return fmt.Errorf("handling slice element: %w", err)
		}

		// Repeated fields can't have default values
		f.DefaultValue = nil
		return nil

	case plenccodec.FieldTypeStruct:
		// It looks like structs inside structs are referenced by type name
		// and inserted at the top level
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
		f.TypeName = proto.String(elt.TypeName)
		if err := b.buildNestedStruct(elt); err != nil {
			return fmt.Errorf("building nested struct: %w", err)
		}

	case plenccodec.FieldTypeBool:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case plenccodec.FieldTypeTime:
		return fmt.Errorf("time type is not supported")
	case plenccodec.FieldTypeJSONObject:
		return fmt.Errorf("JSON object type is not supported")
	case plenccodec.FieldTypeJSONArray:
		return fmt.Errorf("JSON array type is not supported")
	case plenccodec.FieldTypeFlatInt:
		dtyp = descriptorpb.FieldDescriptorProto_TYPE_INT64
	}
	if !elt.ExplicitPresence {
		switch elt.Type {
		case plenccodec.FieldTypeInt, plenccodec.FieldTypeUint, plenccodec.FieldTypeFloat32, plenccodec.FieldTypeFloat64, plenccodec.FieldTypeFlatInt:
			f.DefaultValue = proto.String("0")
		case plenccodec.FieldTypeBool:
			f.DefaultValue = proto.String("false")
		case plenccodec.FieldTypeString:
			f.DefaultValue = proto.String("")
		}
	}
	f.Type = &dtyp
	return nil
}

func (b *descriptorBuilder) buildNestedStruct(pd *plenccodec.Descriptor) error {
	for _, ty := range b.root.NestedType {
		if ty.GetName() == pd.TypeName {
			// Already defined
			return nil
		}
	}
	var nested descriptorpb.DescriptorProto
	nested.Name = proto.String(pd.TypeName)
	b.root.NestedType = append(b.root.NestedType, &nested)

	return b.handleStruct(pd, &nested)
}
