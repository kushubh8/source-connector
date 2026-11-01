package serializer

import "assignment/domain"

type Serializer interface {
	Serialize(domain.Record) ([]byte, error)
}
