package protobson

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	fieldPrefix = "pb_field_"
)

type protobufCodec struct{}

// NewCodec returns a new instance of a BSON codec for Protobuf messages.
// Messages are encoded using field numbers as document keys,
// so that stored messages can survive field renames.
func NewCodec() bsoncodec.ValueCodec {
	return &protobufCodec{}
}

func (pc *protobufCodec) DecodeValue(dctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if val.IsNil() {
		val.Set(reflect.New(val.Type().Elem()))
	}

	dr, err := vr.ReadDocument()
	if err != nil {
		return err
	}

	protoMsg := val.Interface().(proto.Message)
	msg := protoMsg.ProtoReflect()
	for name, vr, err := dr.ReadElement(); err != bsonrw.ErrEOD; name, vr, err = dr.ReadElement() {
		if err != nil {
			return err
		}
		if !strings.HasPrefix(name, fieldPrefix) {
			if err = vr.Skip(); err != nil {
				return err
			}
			continue
		}
		n, err := strconv.Atoi(elementNameToFieldNumber(name))
		if err != nil {
			return err
		}
		num := protoreflect.FieldNumber(n)
		fd := msg.Descriptor().Fields().ByNumber(num)
		// Skip elements representing a field that is not part of the Protobuf message.
		if fd == nil {
			if err = vr.Skip(); err != nil {
				return err
			}
			continue
		}
		fv := msg.NewField(fd)

		// This boolean is used to toggle previous message definition emulation
		// in the decode function.
		// Protobuf specification allows turning a repeated message field into a non-repeated one,
		// and vice-versa, without breaking backwards compatibility.
		// Therefore, if a message with an updated definition containing such change is given as target,
		// a normal decode will fail, and another attempt is made with emulation of previous message definition
		// (i.e. wrap and unwrap fields as necessary). This boolean is used to toggle emulation behavior.
		var emulate bool

		// Try to decode without previous message definition emulation first.
		if err = decodeField(dctx, vr, fd, &fv, emulate); err == nil {
			msg.Set(fd, fv)
			continue
		}
		origErr := err

		// Since initial decode attempt failed, try to decode again with previous message definition emulation.
		// If this attempt also fails, the original decode error is returned.
		emulate = true
		if err = decodeField(dctx, vr, fd, &fv, emulate); err != nil {
			return origErr
		}
		msg.Set(fd, fv)
	}
	return nil
}

func (pc *protobufCodec) EncodeValue(ectx bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	protoMsg := val.Interface().(proto.Message)
	for val.Kind() != reflect.Struct {
		val = val.Elem()
	}

	dw, err := vw.WriteDocument()
	if err != nil {
		return err
	}

	protoMsg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, fv protoreflect.Value) bool {
		if err = encodeField(ectx, dw, fd, &fv); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	return dw.WriteDocumentEnd()
}

// FieldNumberToElementName returns the BSON-encoded field name corresponding to Protobuf message field number.
func FieldNumberToElementName(num protoreflect.FieldNumber) string {
	return fmt.Sprintf("%v%v", fieldPrefix, num)
}

func decodeField(dctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, fd protoreflect.FieldDescriptor, dst *protoreflect.Value, emul bool) error {
	var typ reflect.Type
	var lv protoreflect.List
	var mv protoreflect.Map
	if fd.IsList() {
		// Decoding a list field is done as follows:
		// - without emulation: values are decoded and added to the list normally
		// - with emulation: the single value is wrapped in a list
		lv = dst.List()
		lev := lv.NewElement()
		typ = reflectTypeFromProtoReflectValue(fd, &lev)
		if !emul {
			typ = reflect.SliceOf(typ)
		}
	} else if fd.IsMap() {
		mv = dst.Map()
		msg := dynamicpb.NewMessageType(fd.MapKey().ContainingMessage()).Zero()
		mek, mev := msg.NewField(fd.MapKey()), mv.NewValue()
		mekt, mevt := reflectTypeFromProtoReflectValue(fd.MapKey(), &mek), reflectTypeFromProtoReflectValue(fd.MapValue(), &mev)
		typ = reflect.MapOf(mekt, mevt)
	} else if emul {
		// Decoding a single-value field with emulation is done as follows:
		// - for primitive type fields, the last input value is used
		// - for message type fields, all input values are merged into a single value,
		//   as per proto2 specification: https://developers.google.com/protocol-buffers/docs/proto#updating
		typ = reflect.SliceOf(reflectTypeFromProtoReflectValue(fd, dst))
	} else {
		typ = reflectTypeFromProtoReflectValue(fd, dst)
	}

	dec, err := dctx.LookupDecoder(typ)
	if err != nil {
		return err
	}
	val := reflect.New(typ).Elem()
	if err = dec.DecodeValue(dctx, vr, val); err != nil {
		return err
	}
	if fd.IsList() {
		if emul {
			lv.Append(protoReflectValueFromReflectValue(fd, val))
		} else {
			for i := 0; i < val.Len(); i++ {
				lv.Append(protoReflectValueFromReflectValue(fd, val.Index(i)))
			}
		}
		return nil
	}
	if fd.IsMap() {
		iter := val.MapRange()
		for iter.Next() {
			mek := protoReflectValueFromReflectValue(fd.MapKey(), iter.Key()).MapKey()
			mev := protoReflectValueFromReflectValue(fd.MapValue(), iter.Value())
			mv.Set(mek, mev)
		}
		return nil
	}
	if emul {
		for i := 0; i < val.Len(); i++ {
			ev := val.Index(i)
			if fd.Kind() == protoreflect.MessageKind {
				proto.Merge(dst.Message().Interface(), ev.Interface().(proto.Message))
			} else {
				*dst = protoreflect.ValueOf(ev.Interface())
			}
		}
		return nil
	}
	*dst = protoReflectValueFromReflectValue(fd, val)
	return nil
}

func elementNameToFieldNumber(name string) string {
	return strings.Replace(name, fieldPrefix, "", 1)
}

func encodeField(ectx bsoncodec.EncodeContext, dw bsonrw.DocumentWriter, fd protoreflect.FieldDescriptor, src *protoreflect.Value) error {
	var val reflect.Value
	if fd.IsList() {
		lv := src.List()
		len := lv.Len()
		lev := lv.NewElement()
		typ := reflect.SliceOf(reflectTypeFromProtoReflectValue(fd, &lev))
		sv := reflect.MakeSlice(typ, len, len)
		for i := 0; i < len; i++ {
			lev := lv.Get(i)
			sv.Index(i).Set(reflectValueFromProtoReflectValue(fd, &lev))
		}
		val = sv
	} else if fd.IsMap() {
		pmap := src.Map()
		msg := dynamicpb.NewMessageType(fd.MapKey().ContainingMessage()).Zero()
		mek, mev := msg.NewField(fd.MapKey()), pmap.NewValue()
		mekt, mevt := reflectTypeFromProtoReflectValue(fd.MapKey(), &mek), reflectTypeFromProtoReflectValue(fd.MapValue(), &mev)
		mv := reflect.MakeMapWithSize(reflect.MapOf(mekt, mevt), pmap.Len())
		pmap.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			kv := k.Value()
			key := reflectValueFromProtoReflectValue(fd.MapKey(), &kv)
			val := reflectValueFromProtoReflectValue(fd.MapValue(), &v)
			mv.SetMapIndex(key, val)
			return true
		})
		val = mv
	} else {
		val = reflectValueFromProtoReflectValue(fd, src)
	}

	enc, err := ectx.LookupEncoder(val.Type())
	if err != nil {
		return err
	}

	vw, err := dw.WriteDocumentElement(FieldNumberToElementName(fd.Number()))
	if err != nil {
		return err
	}
	return enc.EncodeValue(ectx, vw, val)
}

func protoReflectValueFromReflectValue(fd protoreflect.FieldDescriptor, v reflect.Value) protoreflect.Value {
	if fd.Message() != nil && !fd.IsMap() {
		return protoreflect.ValueOf(v.Interface().(proto.Message).ProtoReflect())
	}
	return protoreflect.ValueOf(v.Interface())
}

func reflectTypeFromProtoReflectValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value) reflect.Type {
	if fd.Message() != nil && !fd.IsMap() {
		return reflect.TypeOf(v.Message().Interface())
	}
	return reflect.TypeOf(v.Interface())
}

func reflectValueFromProtoReflectValue(fd protoreflect.FieldDescriptor, v *protoreflect.Value) reflect.Value {
	if fd.Message() != nil && !fd.IsMap() {
		return reflect.ValueOf(v.Message().Interface())
	}
	return reflect.ValueOf(v.Interface())
}
