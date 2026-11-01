package retry

import (
	"context"
	"time"
)

// Policy retries errors classified as transient by the supplied predicate.
type Policy struct {
	MaxRetries  int
	Backoff     time.Duration
	IsTransient func(error) bool
}

func (p Policy) Do(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 0; ; attempt++ {
		err = operation()
		if err == nil || p.IsTransient == nil || !p.IsTransient(err) || attempt >= p.MaxRetries {
			return err
		}
		delay := p.Backoff
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
