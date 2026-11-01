package pipeline

import (
	"errors"
	"fmt"

	"assignment/domain"
)

var ErrDropped = errors.New("record intentionally dropped")

type Transform interface {
	Apply(domain.Record) (domain.Record, error)
}

type TransformFactory func() Transform

type TransformRegistry map[string]TransformFactory

func (r TransformRegistry) Build(names []string) ([]Transform, error) {
	out := make([]Transform, 0, len(names))
	for _, name := range names {
		factory, ok := r[name]
		if !ok {
			return nil, fmt.Errorf("unknown transform %q", name)
		}
		out = append(out, factory())
	}
	return out, nil
}

type AddField struct {
	Key   string
	Value any
}

func (t AddField) Apply(r domain.Record) (domain.Record, error) {
	out, err := r.Clone()
	if err != nil {
		return nil, err
	}
	out[t.Key] = t.Value
	return out, nil
}

type DropIfFieldEquals struct {
	Key   string
	Value any
}

func (t DropIfFieldEquals) Apply(r domain.Record) (domain.Record, error) {
	if r[t.Key] == t.Value {
		return nil, ErrDropped
	}
	return r, nil
}
