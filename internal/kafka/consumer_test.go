package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumer_Lifecycle_And_Modes(t *testing.T) {
	tests := []struct {
		name          string
		batchConsumer bool
	}{
		{
			name:          "Batch Consumer Mode",
			batchConsumer: true,
		},
		{
			name:          "Single Poll Consumer Mode",
			batchConsumer: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCluster, err := kafka.NewMockCluster(1)
			if err != nil {
				t.Fatalf("failed to create embedded mock cluster: %v", err)
			}
			defer mockCluster.Close()

			topicName := "test-topic"
			err = mockCluster.CreateTopic(topicName, 1, 1)
			if err != nil {
				t.Fatalf("failed to create topic: %v", err)
			}

			cfg := DefaultKafkaAppConfig()
			cfg.Brokers = mockCluster.BootstrapServers()
			cfg.Consumer.Topics = []string{topicName}
			cfg.Consumer.PollTimeoutMs = 10
			cfg.Consumer.BathPollMaxWaitMs = 10
			cfg.Consumer.BatchSize = 2
			cfg.Consumer.SessionTimeoutMs = 6000
			cfg.Consumer.MaxPollIntervalMs = 10000
			cfg.Consumer.BathcConsumer = tt.batchConsumer

			log := logger.New()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			onMessageFn := func(ctx context.Context, msg *Message) (needCommit bool) {
				return true
			}
			onBatchFn := func(ctx context.Context, msgs []*Message) (needCommit bool) {
				return true
			}

			cons, err := NewConsumer(
				ctx,
				&cfg,
				log,
				ConsumerNameOpt("test-consumer-"+tt.name),
				ConsumerOnMessageOpt(onMessageFn),
				ConsumerOnBatchOpt(onBatchFn),
			)
			if err != nil {
				t.Fatalf("failed to create consumer: %v", err)
			}

			srvConsumer, ok := cons.(*consumer)
			if !ok {
				t.Fatal("failed to assert to *consumer")
			}

			go srvConsumer.Start(ctx)
			time.Sleep(5 * time.Second)

			producerConfig := &kafka.ConfigMap{
				"bootstrap.servers": cfg.Brokers,
			}
			p, err := kafka.NewProducer(producerConfig)
			if err != nil {
				t.Fatalf("failed to create helper producer: %v", err)
			}
			defer p.Close()

			msgToSend := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topicName,
					Partition: 0,
				},
				Value: []byte("test-payload-data"),
			}

			err = p.Produce(msgToSend, nil)
			if err != nil {
				t.Fatalf("failed to produce message to mock cluster: %v", err)
			}
			p.Flush(100)
			time.Sleep(200 * time.Millisecond)

			// 6. Останавливаем
			cancel()

			stopDone := make(chan struct{})
			go func() {
				srvConsumer.Stop()
				close(stopDone)
			}()

			select {
			case <-stopDone:
				// Успешно вышли без дедлока
			case <-time.After(2 * time.Second):
				t.Fatal("deadlock detected during stop!")
			}
		})
	}
}
