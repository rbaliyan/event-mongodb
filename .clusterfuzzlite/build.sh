#!/bin/bash -eu

# Root package (github.com/rbaliyan/event-mongodb)
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzFormatDocumentKey fuzz_format_document_key
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzBsonDToJSON fuzz_bson_d_to_json
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzConvertBSONTypes fuzz_convert_bson_types
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzChangeEventJSON fuzz_change_event_json
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzConvertBSOND fuzz_convert_bson_d
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzFieldCoerce fuzz_field_coerce
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzContextUpdateDescription fuzz_context_update_description
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb FuzzFullDocumentUnmarshal fuzz_full_document_unmarshal

# Subpackages
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/monitor FuzzDecodeMongoCursor fuzz_decode_mongo_cursor
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/distributed FuzzWorkerCursor fuzz_worker_cursor
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/codec FuzzBSONCodecDecode fuzz_bson_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/codec FuzzBSONCodecRoundTrip fuzz_bson_codec_round_trip
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/payload FuzzPayloadBSONDecode fuzz_payload_bson_decode
compile_native_go_fuzzer github.com/rbaliyan/event-mongodb/payload FuzzPayloadBSONRoundTrip fuzz_payload_bson_round_trip

# --- Structure-aware mutation dictionary -------------------------------------
# Ship the shared libFuzzer dictionary and wire it to the byte-oriented targets
# via per-fuzzer .options files, following the OSS-Fuzz/ClusterFuzzLite
# convention ($OUT/<name>.dict + $OUT/<name>.options). Targets whose corpus is
# raw BSON/JSON bytes benefit from the BSON type bytes and field-name tokens;
# scalar-argument targets (e.g. FuzzFieldCoerce) are deliberately omitted.
cp "$SRC"/event-mongodb/.clusterfuzzlite/fuzz.dict "$OUT"/fuzz.dict

byte_oriented_fuzzers=(
  fuzz_bson_d_to_json
  fuzz_convert_bson_types
  fuzz_change_event_json
  fuzz_full_document_unmarshal
  fuzz_decode_mongo_cursor
  fuzz_worker_cursor
  fuzz_bson_codec_decode
  fuzz_bson_codec_round_trip
  fuzz_payload_bson_decode
)

for fuzzer in "${byte_oriented_fuzzers[@]}"; do
  cp "$OUT"/fuzz.dict "$OUT"/"$fuzzer".dict
  cat > "$OUT"/"$fuzzer".options <<EOF
[libfuzzer]
dict = $fuzzer.dict
EOF
done
