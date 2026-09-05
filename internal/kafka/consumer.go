package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	consumerName = "consumer"
	groupId      = "group-id"
	clientId     = "client-id"
)

type Consumer interface {
	Start(ctx context.Context)
	Stop()
}

type consumer struct {
	Logger         *logger.Logger
	consumer       *kafka.Consumer
	cfg            *KafkaAppConfig
	opt            *consumerOptions
	wg             *sync.WaitGroup
	signalStopped  chan struct{}
	lagTicker      *time.Ticker
	knownPartitons map[string]bool // key: "topic/partiton/group"
}

func (c *consumer) withLog() *slog.Logger {
	if c.cfg != nil && c.opt != nil {
		return c.Logger.With(
			consumerName, c.opt.Name,
			groupId, c.cfg.Consumer.GroupId,
			clientId, c.cfg.Consumer.ClientId,
		)
	}
	return c.Logger.Logger
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
		Logger:         log,
		cfg:            cfg,
		opt:            opts,
		wg:             &sync.WaitGroup{},
		signalStopped:  make(chan struct{}),
		lagTicker:      time.NewTicker(30 * time.Second),
		knownPartitons: make(map[string]bool),
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
			cons.withLog().With("err", err.Error()).Error("Failed to set debug config!")
		} else {
			cons.withLog().Info(fmt.Sprintf("kafka debug enabled debug=%v", cfg.Consumer.Debug))
		}
	}

	if cfg.Consumer.EnableRebalaceEvents {
		if err := consumerConfig.SetKey("go.application.rebalance.enable", true); err != nil {
			cons.withLog().With("err", err.Error()).Error("Failed to set rebalance events!")
		} else {
			cons.withLog().Info("Consumer go.application.rebalance.enable=true")
		}
	}
	if cons.consumer, err = kafka.NewConsumer(consumerConfig); err != nil {
		cons.withLog().With("err", err.Error()).Error("Failed to create consumer!")
		return nil, err
	}
	cons.withLog().Info("Success create kafka consumer")

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

				cons.withLog().Log(ctx, level, logEvent.Message,
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
				cons.withLog().Info(fmt.Sprintf("Partition(s) assigned: %+v", convertPartitions(t.Partitions)))
				aerr := cons.consumer.Assign(t.Partitions)
				if aerr != nil {
					cons.withLog().With("err", aerr.Error()).Error("Failed to assign partitions!")
					return aerr
				}
				cons.withLog().Info("Succsessfully assign partitions!")

			case kafka.RevokedPartitions:
				cons.withLog().Info(fmt.Sprintf("Partition(s) revoked: %+v", convertPartitions(t.Partitions)))
				aerr := cons.consumer.Unassign()
				if aerr != nil {
					cons.withLog().With("err", aerr.Error()).Error("Failed to unassign partitions!")
					return aerr
				}
				cons.withLog().Info("Succsessfully unassign partitions!")
			}
			return nil
		}); err != nil {

			cons.withLog().With("topics", cons.cfg.Consumer.Topics, "err", err.Error()).Error("Failed to subscribe topics")
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
			c.withLog().Info("Stop consume messages")
		}
	}()

	defer close(c.signalStopped)
	c.collectConsumerLag(ctx)
	c.withLog().Info("Start consuming messages")

	for {
		select {
		case <-ctx.Done():
			c.withLog().Info("Context canceled, stopping consumer")
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
				c.withLog().Debug(fmt.Sprintf("Received event type: %T", event))
				switch e := event.(type) {
				case *kafka.Message:
					c.handleMessage(ctx, e)
					select {
					case <-ctx.Done():
						return
					default:
						_, err := c.consumer.CommitMessage(e)
						c.handleCommitMessage(err)
					}
				case kafka.Error:
					c.handlerErr(e)
					if e.IsFatal() || strings.Contains(e.Error(), "closed") || strings.Contains(e.Error(), "terminating") {
						c.withLog().Warn("Kafka consumer is closing, exiting loop")
						return
					}
				}

			}
		}
	}
}

func (c *consumer) collectConsumerLag(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return

			case <-c.lagTicker.C:
				c.calculateLag()
			}
		}
	}()
}

func (c *consumer) calculateLag() {
	const (
		defaultTimeout = 2000
	)
	c.withLog().Debug("Start collect lag")
	consumer := c.consumer

	currentAssignment := make(map[string]bool) // key: "topic/partiton/group"

	for _, topic := range c.cfg.Consumer.Topics {
		partitions, err := consumer.Assignment()
		if err != nil {
			c.withLog().With("err", err.Error()).Warn("Caonnot get current assignment!")
			continue
		}
		// register metric for assigned partitons
		consumerAssigenedPartitions.WithLabelValues(topic, c.cfg.Consumer.GroupId).Set(float64(len(partitions)))
		for _, partition := range partitions {
			_, high, err := consumer.GetWatermarkOffsets(topic, partition.Partition)
			if err != nil {
				c.withLog().With("err", err.Error(), "topic", topic, "partiton", partition.Partition).Error("Failed to get watermark!")
				continue
			}
			current, err := consumer.Committed([]kafka.TopicPartition{partition}, defaultTimeout)
			if err != nil {
				c.withLog().With("err", err.Error(), "topic", topic, "partiton", partition.Partition).Error("Failed to get commited!")
				continue

			}
			var currentVal int64 = 0
			if len(current) > 0 {
				currentVal = int64(current[0].Offset)
			}
			lag := float64(high - currentVal)
			if lag < 0 {
				lag = 0
			}
			c.withLog().With("topic", topic, "partiton", partition.Partition, "high", high, "currentVal", currentVal, "lag", lag).Debug("Calculated lag")
			consumerLag.WithLabelValues(topic, fmt.Sprintf("%d", partition.Partition), c.cfg.Consumer.GroupId).Set(lag)
			currentAssignment[fmt.Sprintf("%s/%d/%s", topic, partition.Partition, c.cfg.Consumer.GroupId)] = true
		}
	}

	// clean up lag for partitons  - if not assigned now
	for key := range c.knownPartitons {
		if !currentAssignment[key] {
			keParts := strings.Split(key, "/")
			if len(keParts) == 3 {
				topic := keParts[0]
				partition, err := strconv.Atoi(keParts[1])
				if err == nil {
					group := keParts[2]
					c.withLog().With("topic", topic, "partiton", partition).Debug("Set lag to zero - no longer known partiton")
					consumerLag.WithLabelValues(topic, keParts[1], group).Set(0)
					delete(c.knownPartitons, key)
				}
			}
		}
	}
	// Update current assignment
	for key := range currentAssignment {
		c.knownPartitons[key] = true
	}
}

func (c *consumer) handleCommitMessage(err error) {
	if err != nil {
		c.withLog().With("err", err.Error()).Error("Failed to commmit messages")
		consumerCnt.WithLabelValues(c.opt.Name, "commmited_message", "err").Inc()
		return
	}
	consumerCnt.WithLabelValues(c.opt.Name, "commmited_message", "success").Inc()
	c.withLog().Debug("Commit message")
}

func (c *consumer) handleCommitMessages(ev []*kafka.Message, err error) {
	if err != nil {
		c.withLog().With("err", err.Error()).Error("Failed to commmit messages")
		consumerCnt.WithLabelValues(c.opt.Name, "commmited_message", "err").Add(float64(len(ev)))
		return
	}
	consumerCnt.WithLabelValues(c.opt.Name, "commmited_message", "success").Add(float64(len(ev)))
	c.withLog().Debug("Commit messages")
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

	c.withLog().Info("Stopping kafka consumer...")

	// 1. Первым делом закрываем консьюмер.
	// Это разблокирует Poll() в методе Start() и закроет канал cons.consumer.Logs()!
	if err := c.consumer.Close(); err != nil {
		c.withLog().With("err", err.Error()).Error("Error while closing kafka consumer")
	}

	// 2. Теперь спокойно ждем, когда закроется канал signalStopped (завершится метод Start)
	if c.signalStopped != nil {
		<-c.signalStopped
	}

	// 3. Ждем, пока горутина логирования Logs() закончит читать оставшиеся логи и вызовет wg.Done()
	if c.wg != nil {
		c.wg.Wait()
	}

	c.withLog().Debug("Consumer stopped successfully")
}

func (c *consumer) consumeBatch(ctx context.Context) []*kafka.Message {
	var batch []*kafka.Message
	deadline := time.Now().Add(time.Duration(c.cfg.Consumer.BathPollMaxWaitMs) * time.Millisecond)
	c.withLog().Debug("Start batch consume")
	for len(batch) < c.cfg.Consumer.BatchSize {
		select {
		case <-ctx.Done():
			c.withLog().Debug("Batch sonsume interrupted: context canceled")
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
		c.withLog().Debug(fmt.Sprintf("Received event type: %T", event))
		switch e := event.(type) {
		case *kafka.Message:
			batch = append(batch, e)
		case kafka.Error:
			c.handlerErr(e)
			if e.IsFatal() || strings.Contains(e.Error(), "closed") || strings.Contains(e.Error(), "terminating") {
				c.withLog().Warn("Kafka consumer is closing, exiting loop")
				return batch
			}
		}
	}
	c.withLog().Debug("Batch consume completed")
	return batch
}

func (c *consumer) handlerErr(err error) {
	consumerCnt.WithLabelValues(c.opt.Name, "received_message", "err").Inc()
	c.withLog().With("err", err.Error()).Error("Failed to recieve message")
}

func (c *consumer) handleMessages(ctx context.Context, events []*kafka.Message) {
	consumerCnt.WithLabelValues(c.opt.Name, "received_message", "success").Add(float64(len(events)))
	c.withLog().Debug(fmt.Sprintf("Received %v messages from kafka", len(events)))
	for attempt := 1; uint64(attempt) <= uint64(c.cfg.Consumer.MaxAttempts); attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
			timer := prometheus.NewTimer(consumerHist.WithLabelValues(c.opt.Name, "on_batch"))
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
			timer.ObserveDuration()
			if needCommit {
				return
			}
			consumerCnt.WithLabelValues(c.opt.Name, "processing_message", "retry").Inc()
			c.withLog().With("attempt", attempt).Warn("Processing messages again")
			time.Sleep(time.Duration(c.cfg.Consumer.PollTimeoutMs) * time.Microsecond)
		}
	}
}

func (c *consumer) handleMessage(ctx context.Context, e *kafka.Message) {
	consumerCnt.WithLabelValues(c.opt.Name, "received_message", "success").Inc()
	for attempt := 1; uint64(attempt) <= uint64(c.cfg.Consumer.MaxAttempts); attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
			timer := prometheus.NewTimer(consumerHist.WithLabelValues(c.opt.Name, "on_message"))
			needCommit := c.opt.OnMessage(ctx, &Message{
				e,
				uint64(c.cfg.Consumer.MaxAttempts) - uint64(attempt),
			},
			)
			timer.ObserveDuration()
			if needCommit {
				return
			}
			c.withLog().With("attempt", attempt).Warn("Processing messages again")
			time.Sleep(time.Duration(c.cfg.Consumer.PollTimeoutMs) * time.Microsecond)
		}
	}
}
