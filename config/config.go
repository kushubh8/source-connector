package config

import (
	"errors"
	"fmt"
	"strings"
)

// FeedConfig is the only user-facing configuration for a connector feed.
type FeedConfig struct {
	ID               string   `json:"id"`
	Source           string   `json:"source"` // postgres or http
	Topic            string   `json:"topic"`
	Format           string   `json:"format"` // json or avro
	Transforms       []string `json:"transforms"`
	MaxRetries       int      `json:"maxRetries"`
	RetryBackoffMS   int      `json:"retryBackoffMs"`
	PermanentFailure string   `json:"permanentFailure"` // stop or drop
}

func (c FeedConfig) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return errors.New("feed id is required")
	}
	if strings.TrimSpace(c.Topic) == "" {
		return errors.New("topic is required")
	}
	switch c.Source {
	case "postgres", "http":
	default:
		return fmt.Errorf("unsupported source %q", c.Source)
	}
	switch c.Format {
	case "json", "avro":
	default:
		return fmt.Errorf("unsupported format %q", c.Format)
	}
	if c.MaxRetries < 0 {
		return errors.New("maxRetries cannot be negative")
	}
	if c.RetryBackoffMS < 0 {
		return errors.New("retryBackoffMs cannot be negative")
	}
	if c.PermanentFailure == "" {
		c.PermanentFailure = "stop"
	}
	if c.PermanentFailure != "stop" && c.PermanentFailure != "drop" {
		return errors.New("permanentFailure must be 'stop' or 'drop'")
	}
	return nil
}
