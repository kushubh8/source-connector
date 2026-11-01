package source

import (
	"context"

	"assignment/domain"
)

// Source reads records after a monotonically increasing source position.
// HTTP push does not implement this pull contract; it uses HTTPHandler instead.
type Source interface {
	NextAfter(context.Context, int64) (domain.SourceRecord, error)
}
