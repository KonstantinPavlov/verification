package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const CONSUMER_NAME = "consumer"
const GROUP_ID = "group-id"
const CLIENT_ID = "client-id"

type Consumer interface {
	Start(ctx context.Context)
	Stop()
}

type consumer struct {
	Logger        *logger.Logger
	consumer      *kafka.Consumer
	cfg           *KafkaAppConfig
	opt           *consumerOptions
	wg            *sync.WaitGroup
	signalStopped chan struct{}
}

func NewConsumer(ctx context.Context, cfg *KafkaAppConfig, log *logger.Logger, options ...OptConsumerSetter) (c Consumer, err error) {
	opts := defaultConsumerOptions()
	for _, o := range options {
		o(opts)
	}

	if cfg.Consumer.GroupId == "" {
		cfg.Consumer.GroupId = "test-group"
	}

	if cfg.Consumer.ClientId == "" {
		cfg.Consumer.ClientId = fmt.Sprintf("test-client-%v", uuid.New())
	}
	securityProtocol := "PLAINTEXT"
	if cfg.Tls.Enabled {
		securityProtocol = "SSL"
	}
	cons := &consumer{
		Logger:        log,
		cfg:           cfg,
		opt:           opts,
		wg:            &sync.WaitGroup{},
		signalStopped: make(chan struct{}),
	}

	var consumerConfig = &kafka.ConfigMap{
		"bootstrap.servers":                   cfg.Brokers,
		"group.id":                            cfg.Consumer.GroupId,
		"client.id":                           cfg.Consumer.ClientId,
		"enable.auto.commit":                  false,
		"go.application.rebalance.enable":     false,
		"auto.offset.reset":                   cfg.Consumer.AutoOffsetReset,
		"max.poll.interval.ms":                cfg.Consumer.MaxPollIntervalMs,
		"session.timeout.ms":                  cfg.Consumer.SessionTimeoutMs,
		"fetch.min.bytes":                     cfg.Consumer.FetchMinBytes,
		"fetch.wait.max.ms":                   cfg.Consumer.FetchWaitMaxMs,
		"security.protocol":                   securityProtocol,
		"ssl.ca.location":                     cfg.Tls.CaFilePath,
		"ssl.certificate.location":            cfg.Tls.CertFilePath,
		"ssl.key.location":                    cfg.Tls.KeyFilePath,
		"enable.ssl.certificate.verification": cfg.Tls.SkipTlsVerify,
		"go.logs.channel.enable":              true,
	}

	if cfg.Consumer.Debug != "" {
		if err := consumerConfig.Set("debug=" + cfg.Consumer.Debug); err != nil {
			cons.Logger.With(CONSUMER_NAME, cons.opt.Name, "err", err.Error()).Error("Failed to set debug config!")
		} else {
			cons.Logger.With(CONSUMER_NAME, cons.opt.Name).Info(fmt.Sprintf("kafka debug enabled  debug=%v", cfg.Consumer.Debug))
		}
	}

	if cfg.Consumer.EnableRebalaceEvents {
		if err := consumerConfig.SetKey("go.application.rebalance.enable", true); err != nil {
			cons.Logger.With(CONSUMER_NAME, cons.opt.Name, "err", err.Error()).Error("Failed to set rebalance events!")
		} else {
			cons.Logger.With(CONSUMER_NAME, cons.opt.Name).Info("Consumer go.application.rebalance.enable=true")
		}
	}
	if cons.consumer, err = kafka.NewConsumer(consumerConfig); err != nil {
		cons.Logger.With(CONSUMER_NAME, cons.opt.Name, "err", err.Error()).Error("Failed to create consumer!")
		return nil, err
	}
	cons.Logger.With(CONSUMER_NAME, cons.opt.Name).Info("Success create kafka consumer")

	cons.wg.Add(1)
	go func() {
		defer cons.wg.Done()

		logsChan := cons.consumer.Logs()
		for {
			select {
			case <-ctx.Done():
				return
			case logEvent, ok := <-logsChan:
				if !ok {
					return
				}
				level := slog.LevelInfo
				if logEvent.Level <= 3 {
					level = slog.LevelError
				} else if logEvent.Level <= 4 {
					level = slog.LevelWarn
				} else if logEvent.Level <= 5 {
					level = slog.LevelDebug
				}

				cons.Logger.Log(ctx, level, logEvent.Message,
					CONSUMER_NAME, cons.opt.Name,
					GROUP_ID, cons.cfg.Consumer.GroupId,
					CLIENT_ID, cons.cfg.Consumer.ClientId,
					"kafka_log_tag", logEvent.Tag,
					"kafka_log_name", logEvent.Name,
				)
			}
		}
	}()

	if cfg.Consumer.EnableRebalaceEvents {
		if err := cons.consumer.SubscribeTopics(cfg.Consumer.Topics, func(c *kafka.Consumer, e kafka.Event) error {
			switch t := e.(type) {
			case kafka.AssignedPartitions:
				cons.Logger.With(
					CONSUMER_NAME, cons.opt.Name,
					GROUP_ID, cons.cfg.Consumer.GroupId,
					CLIENT_ID, cons.cfg.Consumer.ClientId,
				).Info(fmt.Sprintf("Partition(s) assigned: %+v", convertPartitions(t.Partitions)))
				aerr := cons.consumer.Assign(t.Partitions)
				if aerr != nil {
					cons.Logger.With(CONSUMER_NAME, cons.opt.Name, GROUP_ID, cons.cfg.Consumer.GroupId,
						CLIENT_ID, cons.cfg.Consumer.ClientId, "err", aerr.Error()).Error("Failed to assign partitions!")
					return aerr
				}
				cons.Logger.With(
					CONSUMER_NAME, cons.opt.Name,
					GROUP_ID, cons.cfg.Consumer.GroupId,
					CLIENT_ID, cons.cfg.Consumer.ClientId,
				).Info("Succsessfully assign partitions!")

			case kafka.RevokedPartitions:
				cons.Logger.With(
					CONSUMER_NAME, cons.opt.Name,
					GROUP_ID, cons.cfg.Consumer.GroupId,
					CLIENT_ID, cons.cfg.Consumer.ClientId,
				).Info(fmt.Sprintf("Partition(s) revoked: %+v", convertPartitions(t.Partitions)))
				aerr := cons.consumer.Unassign()
				if aerr != nil {
					cons.Logger.With(CONSUMER_NAME, cons.opt.Name, GROUP_ID, cons.cfg.Consumer.GroupId,
						CLIENT_ID, cons.cfg.Consumer.ClientId, "err", aerr.Error()).Error("Failed to unassign partitions!")
					return aerr
				}
				cons.Logger.With(
					CONSUMER_NAME, cons.opt.Name,
					GROUP_ID, cons.cfg.Consumer.GroupId,
					CLIENT_ID, cons.cfg.Consumer.ClientId,
				).Info("Succsessfully unassign partitions!")
			}
			return nil
		}); err != nil {

			cons.Logger.With(CONSUMER_NAME, cons.opt.Name, GROUP_ID, cons.cfg.Consumer.GroupId,
				CLIENT_ID, cons.cfg.Consumer.ClientId, "topics", cons.cfg.Consumer.Topics, "err", err.Error()).Error("Failed to subscribe topics")
			return nil, err
		}
	}
	return cons, nil
}

func convertPartitions(partitions []kafka.TopicPartition) []string {
	var res = make([]string, 0, len(partitions))
	for _, part := range partitions {
		topic := *part.Topic
		res = append(res, fmt.Sprintf("%v-%v", topic, part.Partition))
	}
	return res
}

func (c *consumer) Start(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
				CLIENT_ID, c.cfg.Consumer.ClientId).Info("Stop consume messages")
		}
	}()

	defer close(c.signalStopped)
	// TODO Metrics for lag
	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId).Info("Start consuming messages")

	for {
		select {
		case <-ctx.Done():
			c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
				CLIENT_ID, c.cfg.Consumer.ClientId).Info("Context canceled, stopping consumer")
			return
		default:
			if c.cfg.Consumer.BathcConsumer {
				events := c.consumeBatch(ctx)
				if len(events) == 0 {
					continue
				}
				c.handleMessages(ctx, events)
				select {
				//no commit message if ctx is done
				case <-ctx.Done():
					return
				default:
					if len(events) != 0 {
						offsets := dedupOffsets(events)
						_, err := c.consumer.CommitOffsets(offsets)
						c.handleCommitMessages(events, err)
					}
				}
			} else {
				event := c.consumer.Poll(c.cfg.Consumer.PollTimeoutMs)
				if event == nil {
					continue
				}
				c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
					CLIENT_ID, c.cfg.Consumer.ClientId).Debug(fmt.Sprintf("Received event type: %T", event))
				switch e := event.(type) {
				case *kafka.Message:
					c.handleMessage(ctx, e)
					select {
					case <-ctx.Done():
						return
					default:
						_, err := c.consumer.CommitMessage(e)
						c.handleCommitMessage(e, err)
					}
				case kafka.Error:
					c.handlerErr(e)
					if e.IsFatal() || strings.Contains(e.Error(), "closed") || strings.Contains(e.Error(), "terminating") {
						c.Logger.With(CONSUMER_NAME, c.opt.Name).Warn("Kafka consumer is closing, exiting loop")
						return
					}
				}

			}
		}
	}
}

func (c *consumer) handleCommitMessage(e *kafka.Message, err error) {
	// TODO metric for comited messages
	if err != nil {
		c.Logger.With(
			CONSUMER_NAME, c.opt.Name,
			GROUP_ID, c.cfg.Consumer.GroupId,
			CLIENT_ID, c.cfg.Consumer.ClientId,
			"err", err.Error(),
		).Error("Failed to commmit messages")
		return
	}
	c.Logger.With(
		CONSUMER_NAME, c.opt.Name,
		GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId,
	).Debug("Commit message")
}

func (c *consumer) handleCommitMessages(ev []*kafka.Message, err error) {
	// TODO metric for comited messages
	if err != nil {
		c.Logger.With(
			CONSUMER_NAME, c.opt.Name,
			GROUP_ID, c.cfg.Consumer.GroupId,
			CLIENT_ID, c.cfg.Consumer.ClientId,
			"err", err.Error(),
		).Error("Failed to commmit messages")
		return
	}
	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId).Debug("Commit messages")
}

func dedupOffsets(events []*kafka.Message) []kafka.TopicPartition {
	seen := make(map[int32]kafka.TopicPartition)
	for _, event := range events {
		tp := event.TopicPartition
		tp.Offset++
		if existing, ok := seen[tp.Partition]; !ok || tp.Offset > existing.Offset {
			seen[tp.Partition] = tp
		}
	}
	result := make([]kafka.TopicPartition, 0, len(seen))
	for _, tp := range seen {
		result = append(result, tp)
	}
	return result
}
func (c *consumer) Stop() {
	if c == nil {
		return
	}

	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId).Info("Stopping kafka consumer...")

	// 1. Первым делом закрываем консьюмер.
	// Это разблокирует Poll() в методе Start() и закроет канал cons.consumer.Logs()!
	if err := c.consumer.Close(); err != nil {
		c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId, "err", err.Error()).Error("Error while closing kafka consumer")
	}

	// 2. Теперь спокойно ждем, когда закроется канал signalStopped (завершится метод Start)
	if c.signalStopped != nil {
		<-c.signalStopped
	}

	// 3. Ждем, пока горутина логирования Logs() закончит читать оставшиеся логи и вызовет wg.Done()
	if c.wg != nil {
		c.wg.Wait()
	}

	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId).Debug("Consumer stopped successfully")
}

func (c *consumer) consumeBatch(ctx context.Context) []*kafka.Message {
	var batch []*kafka.Message
	deadline := time.Now().Add(time.Duration(c.cfg.Consumer.BathPollMaxWaitMs) * time.Millisecond)
	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId).Debug("Start batch consume")
	for len(batch) < c.cfg.Consumer.BatchSize {
		select {
		case <-ctx.Done():
			c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
				CLIENT_ID, c.cfg.Consumer.ClientId).Debug("Batch sonsume interrupted: context canceled")
			return batch
		default:
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		event := c.consumer.Poll(c.cfg.Consumer.PollTimeoutMs)
		if event == nil {
			continue
		}
		c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
			CLIENT_ID, c.cfg.Consumer.ClientId).Debug(fmt.Sprintf("Received event type: %T", event))
		switch e := event.(type) {
		case *kafka.Message:
			batch = append(batch, e)
		case kafka.Error:
			c.handlerErr(e)
			if e.IsFatal() || strings.Contains(e.Error(), "closed") || strings.Contains(e.Error(), "terminating") {
				c.Logger.With(CONSUMER_NAME, c.opt.Name).Warn("Kafka consumer is closing, exiting loop")
				return batch
			}
		}
	}
	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId).Debug("Batch consume completed")
	return batch
}

func (c *consumer) handlerErr(err error) {
	// TODO metric received_mesages error
	c.Logger.With(
		CONSUMER_NAME, c.opt.Name,
		GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId,
		"err", err.Error(),
	).Error("Failed to recieve message")
}

func (c *consumer) handleMessages(ctx context.Context, events []*kafka.Message) {
	//TODO counter for received messages
	c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
		CLIENT_ID, c.cfg.Consumer.ClientId).Debug(fmt.Sprintf("Received %v messages from kafka", len(events)))

	for attempt := 1; uint64(attempt) <= uint64(c.cfg.Consumer.MaxAttempts); attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
			// TODO on batch timer metric
			var batch []*Message
			for _, m := range events {
				batch = append(batch,
					&Message{
						m,
						uint64(c.cfg.Consumer.MaxAttempts) - uint64(attempt),
					})
			}
			// invoke process function
			needCommit := c.opt.OnBatch(ctx, batch)
			//TODO  observe duration
			if needCommit {
				return
			}
			c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
				CLIENT_ID, c.cfg.Consumer.ClientId, "attempt", attempt).Warn("Processing messages again")
			time.Sleep(time.Duration(c.cfg.Consumer.PollTimeoutMs) * time.Microsecond)
		}
	}
}

func (c *consumer) handleMessage(ctx context.Context, e *kafka.Message) {
	// TODO counter received_messages
	for attempt := 1; uint64(attempt) <= uint64(c.cfg.Consumer.MaxAttempts); attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
			// TODO on message timer metric
			needCommit := c.opt.OnMessage(ctx, &Message{
				e,
				uint64(c.cfg.Consumer.MaxAttempts) - uint64(attempt),
			},
			)
			//TODO  observe duration
			if needCommit {
				return
			}
			c.Logger.With(CONSUMER_NAME, c.opt.Name, GROUP_ID, c.cfg.Consumer.GroupId,
				CLIENT_ID, c.cfg.Consumer.ClientId, "attempt", attempt).Warn("Processing messages again")
			time.Sleep(time.Duration(c.cfg.Consumer.PollTimeoutMs) * time.Microsecond)
		}
	}
}
