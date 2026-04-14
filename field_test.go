package mongodb

import "testing"

func TestField_directType(t *testing.T) {
	desc := &UpdateDescription{
		UpdatedFields: map[string]any{
			"name":   "alice",
			"active": true,
		},
	}

	name, ok := Field[string](desc, "name")
	if !ok || name != "alice" {
		t.Errorf("Field[string](name) = (%q, %v), want (alice, true)", name, ok)
	}

	active, ok := Field[bool](desc, "active")
	if !ok || !active {
		t.Errorf("Field[bool](active) = (%v, %v), want (true, true)", active, ok)
	}

	_, ok = Field[string](desc, "missing")
	if ok {
		t.Error("Field on missing key returned ok=true")
	}
}

func TestField_numericCoercion(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		wantI  int64
		wantF  float64
		wantI32 int32
	}{
		{"float64 (JSON path)", float64(42), 42, 42.0, 42},
		{"int32 (BSON path)", int32(7), 7, 7.0, 7},
		{"int64 (BSON path)", int64(99), 99, 99.0, 99},
		{"int", int(10), 10, 10.0, 10},
		{"float32", float32(3.14), 3, 3.140000104904175, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := &UpdateDescription{
				UpdatedFields: map[string]any{"v": tt.value},
			}

			gotI, ok := Field[int64](desc, "v")
			if !ok || gotI != tt.wantI {
				t.Errorf("Field[int64] = (%d, %v), want (%d, true)", gotI, ok, tt.wantI)
			}

			gotF, ok := Field[float64](desc, "v")
			if !ok || gotF != tt.wantF {
				t.Errorf("Field[float64] = (%f, %v), want (%f, true)", gotF, ok, tt.wantF)
			}

			gotI32, ok := Field[int32](desc, "v")
			if !ok || gotI32 != tt.wantI32 {
				t.Errorf("Field[int32] = (%d, %v), want (%d, true)", gotI32, ok, tt.wantI32)
			}
		})
	}
}

func TestField_nonNumericCoercionFails(t *testing.T) {
	desc := &UpdateDescription{
		UpdatedFields: map[string]any{"name": "alice"},
	}

	// string → int64 should fail
	_, ok := Field[int64](desc, "name")
	if ok {
		t.Error("string → int64 coercion should not succeed")
	}

	// float64 → string should fail
	desc.UpdatedFields["count"] = float64(5)
	_, ok = Field[string](desc, "count")
	if ok {
		t.Error("float64 → string coercion should not succeed")
	}
}

func TestField_nilReceiver(t *testing.T) {
	v, ok := Field[string](nil, "key")
	if ok || v != "" {
		t.Errorf("Field on nil = (%q, %v), want (\"\", false)", v, ok)
	}
}

func TestField_nilValue(t *testing.T) {
	desc := &UpdateDescription{
		UpdatedFields: map[string]any{"key": nil},
	}
	_, ok := Field[string](desc, "key")
	if ok {
		t.Error("Field on nil value should return false")
	}
}

func TestHasField(t *testing.T) {
	desc := &UpdateDescription{
		UpdatedFields: map[string]any{"name": "alice", "age": nil},
	}

	if !desc.HasField("name") {
		t.Error("HasField(name) = false, want true")
	}
	if !desc.HasField("age") {
		t.Error("HasField(age) with nil value = false, want true")
	}
	if desc.HasField("missing") {
		t.Error("HasField(missing) = true, want false")
	}

	var nilDesc *UpdateDescription
	if nilDesc.HasField("key") {
		t.Error("HasField on nil receiver = true, want false")
	}
}

func TestHasFieldRemoved(t *testing.T) {
	desc := &UpdateDescription{
		RemovedFields: []string{"old_field", "deprecated"},
	}

	if !desc.HasFieldRemoved("old_field") {
		t.Error("HasFieldRemoved(old_field) = false, want true")
	}
	if desc.HasFieldRemoved("kept") {
		t.Error("HasFieldRemoved(kept) = true, want false")
	}

	var nilDesc *UpdateDescription
	if nilDesc.HasFieldRemoved("key") {
		t.Error("HasFieldRemoved on nil receiver = true, want false")
	}
}
