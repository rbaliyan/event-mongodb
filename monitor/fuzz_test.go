package monitor

import "testing"

// FuzzDecodeMongoCursor feeds arbitrary strings to decodeMongoCursor.
//
// Oracle:
//   - decodeMongoCursor must never panic on any input.
//   - When decoding succeeds, re-encoding the result and decoding again must be
//     stable (the second decode succeeds and yields an equal cursor). This
//     guards the encode/decode round-trip invariant the List pagination cursor
//     relies on.
func FuzzDecodeMongoCursor(f *testing.F) {
	seeds := []string{
		"",
		encodeMongoCursor(mongoCursor{}),
		encodeMongoCursor(mongoCursor{EventID: "evt-1", SubID: "sub-1"}),
		"not-base64-!!!",
		"eyJzIjoiMjAyNi0wNi0xOVQxMDowMDowMFoifQ==", // base64 of valid JSON object
		"e30=", // base64 of "{}"
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		cur, err := decodeMongoCursor(in)
		if err != nil {
			// Invalid input rejected cleanly; nothing more to check.
			return
		}

		// Round-trip stability: re-encode and re-decode must succeed and match.
		reencoded := encodeMongoCursor(cur)
		cur2, err := decodeMongoCursor(reencoded)
		if err != nil {
			t.Fatalf("re-decode of re-encoded cursor failed: %v (input=%q)", err, in)
		}
		if !cur2.StartedAt.Equal(cur.StartedAt) {
			t.Errorf("StartedAt not stable: %v vs %v (input=%q)", cur2.StartedAt, cur.StartedAt, in)
		}
		if cur2.EventID != cur.EventID {
			t.Errorf("EventID not stable: %q vs %q (input=%q)", cur2.EventID, cur.EventID, in)
		}
		if cur2.SubID != cur.SubID {
			t.Errorf("SubID not stable: %q vs %q (input=%q)", cur2.SubID, cur.SubID, in)
		}
	})
}
