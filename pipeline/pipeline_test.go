package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"assignment/domain"
	"assignment/publisher"
	"assignment/retry"
	"assignment/serializer"
)

func TestPipelinePublishesTransformedRecord(t *testing.T) {
	kafka := &publisher.MemoryKafka{}
	p, err := New(kafka, serializer.JSON{}, []Transform{AddField{Key: "x", Value: 1}}, retry.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Process(context.Background(), "topic", domain.Record{"id": 7}); err != nil {
		t.Fatal(err)
	}
	messages := kafka.Snapshot()
	if len(messages) != 1 {
		t.Fatalf("got %d messages", len(messages))
	}
	var got map[string]any
	if err := json.Unmarshal(messages[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["x"] != float64(1) {
		t.Fatalf("transform missing: %#v", got)
	}
}

func TestPipelineDropDoesNotPublish(t *testing.T) {
	kafka := &publisher.MemoryKafka{}
	p, _ := New(kafka, serializer.JSON{}, []Transform{DropIfFieldEquals{Key: "skip", Value: true}}, retry.Policy{})
	err := p.Process(context.Background(), "topic", domain.Record{"skip": true})
	if !errors.Is(err, ErrDropped) {
		t.Fatalf("expected ErrDropped, got %v", err)
	}
	if len(kafka.Snapshot()) != 0 {
		t.Fatal("dropped record was published")
	}
}

func TestPipelineRetriesTransientPublisherFailure(t *testing.T) {
	kafka := &publisher.MemoryKafka{FailWith: publisher.ErrUnavailable}
	p, _ := New(kafka, serializer.JSON{}, nil, retry.Policy{
		MaxRetries:  2,
		IsTransient: func(err error) bool { return errors.Is(err, publisher.ErrUnavailable) },
	})
	err := p.Process(context.Background(), "topic", domain.Record{"id": 1})
	if !errors.Is(err, publisher.ErrUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}
