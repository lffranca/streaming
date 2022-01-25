package streaming

import (
	"github.com/segmentio/kafka-go"
	"time"
)

type ConsumerHandler func(message Message) error

func MessageFromKafka(item kafka.Message) Message {
	return Message{
		Topic:         item.Topic,
		Partition:     item.Partition,
		Offset:        item.Offset,
		HighWaterMark: item.HighWaterMark,
		Key:           item.Key,
		Value:         item.Value,
		Headers:       HeadersFromKafka(item.Headers),
		Time:          item.Time,
	}
}

type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []Header
	Time          time.Time
}

func HeadersFromKafka(items []kafka.Header) (values []Header) {
	for _, item := range items {
		values = append(values, HeaderFromKafka(item))
	}

	return
}

func HeaderFromKafka(item kafka.Header) Header {
	return Header{
		Key:   item.Key,
		Value: item.Value,
	}
}

type Header struct {
	Key   string
	Value []byte
}
