package streaming

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"strings"
)

func NewKafka(network string, brokers []string) (Streaming, error) {
	client := new(clientKafka)
	client.network = network
	client.brokers = brokers

	return client, nil
}

type clientKafka struct {
	network string
	brokers []string
}

func (pkg *clientKafka) Producer(ctx context.Context, topic string, value []byte) (err error) {
	conn, err := kafka.DialLeader(
		ctx,
		pkg.network,
		strings.Join(pkg.brokers, ","),
		topic,
		0,
	)
	if err != nil {
		return err
	}

	defer func() {
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()

	if _, err = conn.Write(value); err != nil {
		return err
	}

	return
}

func (pkg *clientKafka) Consumer(ctx context.Context, topic string, handler ConsumerHandler) (err error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   pkg.brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	offsetLast, err := r.ReadLag(ctx)
	if err != nil {
		return err
	}

	if err := r.SetOffset(offsetLast); err != nil {
		return err
	}

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}

		if err = handler(MessageFromKafka(m)); err != nil {
			return err
		}
	}

	if err := r.Close(); err != nil {
		return err
	}

	return
}
