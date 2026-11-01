package source

import (
	"context"
	"errors"
	"sort"
	"sync"

	"assignment/domain"
)

var ErrNoRecords = errors.New("no more records")

// MemoryPostgres is a deterministic Postgres stand-in. Rows are ordered by ID.
type MemoryPostgres struct {
	mu   sync.RWMutex
	rows []domain.SourceRecord
}

func NewMemoryPostgres(rows []domain.SourceRecord) *MemoryPostgres {
	copyRows := append([]domain.SourceRecord(nil), rows...)
	sort.Slice(copyRows, func(i, j int) bool { return copyRows[i].Position < copyRows[j].Position })
	return &MemoryPostgres{rows: copyRows}
}

func (s *MemoryPostgres) NextAfter(ctx context.Context, position int64) (domain.SourceRecord, error) {
	if err := ctx.Err(); err != nil {
		return domain.SourceRecord{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, row := range s.rows {
		if row.Position > position {
			copyRecord, err := row.Record.Clone()
			if err != nil {
				return domain.SourceRecord{}, err
			}
			return domain.SourceRecord{Record: copyRecord, Position: row.Position}, nil
		}
	}
	return domain.SourceRecord{}, ErrNoRecords
}
