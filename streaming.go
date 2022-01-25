package streaming

import "context"

type Streaming interface {
	Producer(ctx context.Context, topic string, value []byte) (err error)
	Consumer(ctx context.Context, topic string, handler ConsumerHandler) (err error)
}
