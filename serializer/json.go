package serializer

import (
	"encoding/json"

	"assignment/domain"
)

type JSON struct{}

func (JSON) Serialize(r domain.Record) ([]byte, error) {
	return json.Marshal(r)
}
