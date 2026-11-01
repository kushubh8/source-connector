package pipeline

import (
	"context"
	"errors"
	"fmt"

	"assignment/domain"
	"assignment/publisher"
	"assignment/retry"
	"assignment/serializer"
)

type Pipeline struct {
	publisher  publisher.Publisher
	serializer serializer.Serializer
	transforms []Transform
	retry      retry.Policy
}

func New(p publisher.Publisher, s serializer.Serializer, transforms []Transform, retryPolicy retry.Policy) (*Pipeline, error) {
	if p == nil {
		return nil, errors.New("publisher is required")
	}
	if s == nil {
		return nil, errors.New("serializer is required")
	}
	return &Pipeline{publisher: p, serializer: s, transforms: append([]Transform(nil), transforms...), retry: retryPolicy}, nil
}

// Process returns nil when published, ErrDropped when intentionally dropped,
// and another error when processing failed. Callers must only checkpoint nil/ErrDropped.
func (p *Pipeline) Process(ctx context.Context, topic string, input domain.Record) error {
	record, err := input.Clone()
	if err != nil {
		return fmt.Errorf("clone record: %w", err)
	}
	for i, transform := range p.transforms {
		next, err := transform.Apply(record)
		if errors.Is(err, ErrDropped) {
			return ErrDropped
		}
		if err != nil {
			return fmt.Errorf("transform %d: %w", i, err)
		}
		if next == nil {
			return ErrDropped
		}
		record = next
	}
	data, err := p.serializer.Serialize(record)
	if err != nil {
		return fmt.Errorf("serialize record: %w", err)
	}
	if err := p.retry.Do(ctx, func() error { return p.publisher.Publish(ctx, topic, data) }); err != nil {
		return fmt.Errorf("publish record: %w", err)
	}
	return nil
}
