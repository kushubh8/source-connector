package connector

import (
	"context"
	"errors"
	"testing"

	"assignment/checkpoint"
	"assignment/config"
	"assignment/domain"
	"assignment/pipeline"
	"assignment/publisher"
	"assignment/retry"
	"assignment/serializer"
	"assignment/source"
)

func TestRunnerAdvancesCheckpointAfterPublish(t *testing.T) {
	feed := config.FeedConfig{ID: "f", Source: "postgres", Topic: "t", Format: "json", PermanentFailure: "stop"}
	kafka := &publisher.MemoryKafka{}
	p, _ := pipeline.New(kafka, serializer.JSON{}, nil, retry.Policy{})
	src := source.NewMemoryPostgres([]domain.SourceRecord{{Position: 4, Record: domain.Record{"id": 1}}})
	progress := checkpoint.NewMemoryStore()
	r := Runner{Feed: feed, Source: src, Progress: progress, Pipeline: p}
	if err := r.RunOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress.Load("f") != 4 {
		t.Fatalf("checkpoint=%d", progress.Load("f"))
	}
	if len(kafka.Snapshot()) != 1 {
		t.Fatal("record not published")
	}
}

func TestRunnerDoesNotAdvanceOnKafkaFailure(t *testing.T) {
	feed := config.FeedConfig{ID: "f", Source: "postgres", Topic: "t", Format: "json", PermanentFailure: "drop"}
	kafka := &publisher.MemoryKafka{FailWith: publisher.ErrUnavailable}
	p, _ := pipeline.New(kafka, serializer.JSON{}, nil, retry.Policy{IsTransient: func(err error) bool { return errors.Is(err, publisher.ErrUnavailable) }})
	src := source.NewMemoryPostgres([]domain.SourceRecord{{Position: 4, Record: domain.Record{"id": 1}}})
	progress := checkpoint.NewMemoryStore()
	r := Runner{Feed: feed, Source: src, Progress: progress, Pipeline: p}
	if err := r.RunOne(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if progress.Load("f") != 0 {
		t.Fatal("checkpoint advanced after Kafka failure")
	}
}
