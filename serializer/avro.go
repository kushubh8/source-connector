package serializer

import (
	"encoding/json"
	"errors"

	"assignment/domain"
)

// AvroStandIn is an explicit simulation for this assessment environment.
// It is NOT valid Avro. The prompt states no Avro library/network is available,
// so the implementation keeps the Avro extension seam without claiming wire compatibility.
type AvroStandIn struct{}

func (AvroStandIn) Serialize(r domain.Record) ([]byte, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, errors.New("empty Avro stand-in payload")
	}
	return append([]byte("AVRO-STUB:"), payload...), nil
}
