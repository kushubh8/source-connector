package connector

import (
	"context"
	"errors"
	"fmt"

	"assignment/checkpoint"
	"assignment/config"
	"assignment/pipeline"
	"assignment/publisher"
	"assignment/source"
)

type Runner struct {
	Feed     config.FeedConfig
	Source   source.Source
	Progress checkpoint.Store
	Pipeline *pipeline.Pipeline
}

// RunOne processes at most one source record. It is deliberately small so a
// scheduler/worker can decide polling cadence without changing domain logic.
func (r *Runner) RunOne(ctx context.Context) error {
	if r.Source == nil || r.Progress == nil || r.Pipeline == nil {
		return errors.New("runner dependencies are required")
	}
	position := r.Progress.Load(r.Feed.ID)
	item, err := r.Source.NextAfter(ctx, position)
	if err != nil {
		return err
	}
	if item.Position <= position {
		return fmt.Errorf("source position %d must be greater than checkpoint %d", item.Position, position)
	}

	err = r.Pipeline.Process(ctx, r.Feed.Topic, item.Record)
	if errors.Is(err, pipeline.ErrDropped) {
		return r.Progress.Save(r.Feed.ID, item.Position)
	}
	if err != nil {
		// A Kafka outage is not a permanently bad record: never skip it.
		if errors.Is(err, publisher.ErrUnavailable) || r.Feed.PermanentFailure != "drop" {
			return err
		}
		// Configured policy: skip a permanently unprocessable record.
		// In production, this should also emit a DLQ/audit event.
		return r.Progress.Save(r.Feed.ID, item.Position)
	}
	return r.Progress.Save(r.Feed.ID, item.Position)
}

func (r *Runner) Run(ctx context.Context) error {
	for {
		err := r.RunOne(ctx)
		if errors.Is(err, source.ErrNoRecords) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
