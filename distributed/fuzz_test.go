package distributed

import "testing"

// FuzzWorkerCursor feeds arbitrary strings to decodeWorkerCursor, the pagination
// cursor decoder for ListWorkers. The cursor is attacker-influenceable (it
// arrives over the wire in WorkerFilter.Cursor), so it must be hardened against
// hostile input.
//
// Oracle (mirrors monitor FuzzDecodeMongoCursor):
//   - decodeWorkerCursor must never panic on any input.
//   - When decoding succeeds, re-encoding the result and decoding again must be
//     stable: the second decode succeeds and yields an equal cursor. This guards
//     the encode/decode round-trip invariant the keyset pagination relies on.
func FuzzWorkerCursor(f *testing.F) {
	seeds := []string{
		"",
		encodeWorkerCursor(workerCursor{}),
		encodeWorkerCursor(workerCursor{ID: "msg-1"}),
		"not-base64-!!!",
		"e30=",     // base64 of "{}"
		"bnVsbA==", // base64 of "null"
		"W10=",     // base64 of "[]" (type-mismatched JSON)
		"eyJ1IjoiMjAyNi0wNi0xOVQxMDowMDowMFoiLCJpIjoibXNnLTEifQ==", // base64 of a valid cursor object
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		cur, err := decodeWorkerCursor(in)
		if err != nil {
			// Invalid input rejected cleanly; nothing more to check.
			return
		}

		// Round-trip stability: re-encode and re-decode must succeed and match.
		reencoded := encodeWorkerCursor(cur)
		cur2, err := decodeWorkerCursor(reencoded)
		if err != nil {
			t.Fatalf("re-decode of re-encoded cursor failed: %v (input=%q)", err, in)
		}
		if !cur2.UpdatedAt.Equal(cur.UpdatedAt) {
			t.Errorf("UpdatedAt not stable: %v vs %v (input=%q)", cur2.UpdatedAt, cur.UpdatedAt, in)
		}
		if cur2.ID != cur.ID {
			t.Errorf("ID not stable: %q vs %q (input=%q)", cur2.ID, cur.ID, in)
		}
	})
}
