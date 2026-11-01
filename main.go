package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"assignment/checkpoint"
	"assignment/config"
	"assignment/connector"
	"assignment/domain"
	"assignment/pipeline"
	"assignment/publisher"
	"assignment/retry"
	"assignment/serializer"
	"assignment/source"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	feed := config.FeedConfig{
		ID: "orders-feed", Source: "postgres", Topic: "orders",
		Format: "json", Transforms: []string{"add-source"},
		MaxRetries: 2, RetryBackoffMS: 10, PermanentFailure: "stop",
	}
	if err := feed.Validate(); err != nil {
		panic(err)
	}

	registry := pipeline.TransformRegistry{
		"add-source": func() pipeline.Transform {
			return pipeline.AddField{Key: "connectorSource", Value: "framework"}
		},
		"drop-test-records": func() pipeline.Transform {
			return pipeline.DropIfFieldEquals{Key: "test", Value: true}
		},
	}
	transforms, err := registry.Build(feed.Transforms)
	if err != nil {
		panic(err)
	}

	var enc serializer.Serializer
	switch feed.Format {
	case "json":
		enc = serializer.JSON{}
	case "avro":
		enc = serializer.AvroStandIn{}
	default:
		panic("unsupported serializer")
	}

	kafka := &publisher.MemoryKafka{}
	policy := retry.Policy{
		MaxRetries:  feed.MaxRetries,
		Backoff:     time.Duration(feed.RetryBackoffMS) * time.Millisecond,
		IsTransient: func(err error) bool { return errors.Is(err, publisher.ErrUnavailable) },
	}
	pipe, err := pipeline.New(kafka, enc, transforms, policy)
	if err != nil {
		panic(err)
	}

	postgres := source.NewMemoryPostgres([]domain.SourceRecord{
		{Position: 1, Record: domain.Record{"orderId": 1001, "amount": 42.5}},
		{Position: 2, Record: domain.Record{"orderId": 1002, "amount": 99.0}},
		{Position: 3, Record: domain.Record{"orderId": 1003, "test": true}},
	})
	progress := checkpoint.NewMemoryStore()
	runner := &connector.Runner{Feed: feed, Source: postgres, Progress: progress, Pipeline: pipe}
	if err := runner.Run(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("published=%d checkpoint=%d\n", len(kafka.Snapshot()), progress.Load(feed.ID))

	// The same pipeline is used by the HTTP push adapter; no source-specific
	// processing logic is added to the pipeline.
	_ = source.HTTPHandler{Topic: feed.Topic, Pipeline: pipe}
}
