package domain

import "encoding/json"

// Record intentionally uses a generic map because source schemas vary by feed.
type Record map[string]any

// Clone performs a JSON-compatible deep copy, preventing transforms from
// unexpectedly mutating the source adapter's original record.
func (r Record) Clone() (Record, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	var out Record
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type SourceRecord struct {
	Record   Record
	Position int64
}
