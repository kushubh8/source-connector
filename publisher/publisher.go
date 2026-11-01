package publisher

import (
	"context"
	"errors"
	"sync"
)

var ErrUnavailable = errors.New("publisher unavailable")
var ErrRejected = errors.New("publisher rejected message")

type Publisher interface {
	Publish(context.Context, string, []byte) error
}

type Message struct {
	Topic string
	Value []byte
}

// MemoryKafka simulates Kafka and can be configured to fail on demand.
type MemoryKafka struct {
	mu       sync.Mutex
	Messages []Message
	FailWith error
}

func (k *MemoryKafka) Publish(ctx context.Context, topic string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.FailWith != nil {
		return k.FailWith
	}
	copyValue := append([]byte(nil), value...)
	k.Messages = append(k.Messages, Message{Topic: topic, Value: copyValue})
	return nil
}

func (k *MemoryKafka) Snapshot() []Message {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]Message, len(k.Messages))
	for i, m := range k.Messages {
		out[i] = Message{Topic: m.Topic, Value: append([]byte(nil), m.Value...)}
	}
	return out
}
