package kafka

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type Producer interface {
	Start(ctx context.Context) (err error)
	Produce(msg *kafka.Message) (err error)
	ProduceAndGet(ctx context.Context, msg *kafka.Message) (err error)
	ProduceBatch(msgs []*kafka.Message) (err error)
	Stop()
}

type producer struct {
	cfg        *KafkaAppConfig
	clientId   string
	mu         sync.RWMutex
	producer   *kafka.Producer
	log        *logger.Logger
	wg         *sync.WaitGroup
	deliveryWg *sync.WaitGroup
}

func NewProducer(cfg *KafkaAppConfig, log *logger.Logger) (p Producer) {
	var clientId = cfg.Producer.ClientId
	if clientId == "" {
		var hostName = os.Getenv("HOSTNAME")
		if hostName != "" {
			clientId = hostName
		}
	}
	return &producer{
		cfg:        cfg,
		log:        log,
		clientId:   clientId,
		wg:         &sync.WaitGroup{},
		deliveryWg: &sync.WaitGroup{},
	}
}

func (p *producer) Start(ctx context.Context) (err error) {
	securityProtocol := "PLAINTEXT"
	if p.cfg.Tls.Enabled {
		securityProtocol = "SSL"
	}
	var producerConfig = kafka.ConfigMap{
		"bootstrap.servers":                   p.cfg.Brokers,
		"client.id":                           p.clientId,
		"acks":                                p.cfg.Producer.Acks,
		"security.protocol":                   securityProtocol,
		"ssl.ca.location":                     p.cfg.Tls.CaFilePath,
		"ssl.certificate.location":            p.cfg.Tls.CertFilePath,
		"ssl.key.location":                    p.cfg.Tls.KeyFilePath,
		"enable.ssl.certificate.verification": p.cfg.Tls.SkipTlsVerify,
		"go.logs.channel.enable":              true,
		"go.delivery.reports":                 true,
	}
	p.log.Info("Creating kafka producer")

	producer, err := kafka.NewProducer(&producerConfig)
	if err != nil {
		p.log.Error("Failed to create Kafka producer!", "err", err.Error())
		return err
	}

	p.mu.Lock()
	p.producer = producer
	p.mu.Unlock()

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		p.mu.RLock()
		if p.producer == nil {
			p.mu.RUnlock()
			return
		}
		logsChan := p.producer.Logs()
		p.mu.RUnlock()
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
				p.log.Log(ctx, level, logEvent.Message,
					"kafka_log_tag", logEvent.Tag,
					"kafka_log_name", logEvent.Name,
				)
			}
		}
	}()
	p.deliveryWg.Add(1)
	go p.handleDeliveryReports(ctx)
	p.log.Info("kafkfa producer started successfully", "brokers", p.cfg.Brokers)
	return nil
}

func (p *producer) handleDeliveryReports(ctx context.Context) {
	defer p.deliveryWg.Done()
	p.mu.RLock()
	if p.producer == nil {
		p.mu.RUnlock()
		return
	}
	eventsChan := p.producer.Events()
	p.mu.RUnlock()
	select {
	case <-ctx.Done():
		return
	case event, ok := <-eventsChan:
		if !ok {
			return
		}
		switch ev := event.(type) {
		case *kafka.Message:
			topic := ""
			if ev.TopicPartition.Topic != nil {
				topic = *ev.TopicPartition.Topic
			}
			if ev.TopicPartition.Error != nil {
				p.log.Error("Async message delivery failed!", "topic", topic, "partiton", ev.TopicPartition.Partition, "err", ev.TopicPartition.Error.Error())
				producerCnt.WithLabelValues(p.clientId, topic, "delivery", "failure").Inc()
			} else {
				producerCnt.WithLabelValues(p.clientId, topic, "delivery", "success").Inc()
			}
		}
	}
}

func (p *producer) Produce(msg *kafka.Message) (err error) {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	p.mu.RLock()
	prod := p.producer
	p.mu.RUnlock()

	if prod == nil {
		p.log.Error("Async producer failed! Producer not initialized!")
		producerCnt.WithLabelValues(p.clientId, topic, "produce", "failure").Inc()
		return errors.New("Producer not initialized!")
	}
	timer := prometheus.NewTimer(producerHist.WithLabelValues(p.clientId, topic, "produce"))
	defer timer.ObserveDuration()
	if err := prod.Produce(msg, nil); err != nil {
		p.log.Error("Async produce failed!",
			"topic", topic,
			"key", string(msg.Key),
			"err", err.Error(),
		)
		producerCnt.WithLabelValues(p.clientId, topic, "produce", "failure").Inc()

		return err
	} else {
		p.log.Info("Message to kafka sent", "topic", topic, "key", string(msg.Key), "value_len", len(msg.Value), "headers_count", len(msg.Headers))
		producerCnt.WithLabelValues(p.clientId, topic, "produce", "success").Inc()
	}
	return nil
}

func (p *producer) ProduceAndGet(ctx context.Context, msg *kafka.Message) (err error) {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	p.mu.RLock()
	prod := p.producer
	p.mu.RUnlock()

	if prod == nil {
		p.log.Error("Async producer failed! Producer not initialized!")
		producerCnt.WithLabelValues(p.clientId, topic, "produce", "failure").Inc()
		return errors.New("Producer not initialized!")
	}

	p.log.Info("Producing message to kafka", "topic", topic, "key", string(msg.Key), "value_len", len(msg.Value), "headers_count", len(msg.Headers))

	delivery := make(chan kafka.Event, 1)
	defer close(delivery)
	timer := prometheus.NewTimer(producerHist.WithLabelValues(p.clientId, topic, "produce_and_get"))
	defer timer.ObserveDuration()
	if err := prod.Produce(msg, delivery); err != nil {
		p.log.Error("Produce failed!",
			"topic", topic,
			"key", string(msg.Key),
			"err", err.Error(),
		)
		producerCnt.WithLabelValues(p.clientId, topic, "produce", "failure").Inc()

		return err
	}
	producerCnt.WithLabelValues(p.clientId, topic, "produce", "success").Inc()
	select {
	case <-ctx.Done():
		p.log.Warn("ProduceAndGet interrupted: Context canceled!")
		producerCnt.WithLabelValues(p.clientId, topic, "delievery", "interrupted").Inc()
		p.producer.Flush(p.cfg.Producer.SendTimeoutMs)
		return ctx.Err()
	case event := <-delivery:
		if msg, ok := event.(*kafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				p.log.Error("Failed to deliver message",
					"topic", topic,
					"key", string(msg.Key), "err", msg.TopicPartition.Error.Error())

				producerCnt.WithLabelValues(p.clientId, topic, "delievery", "failure").Inc()
				return msg.TopicPartition.Error
			}
			p.log.Info("Message delivered to kafka",
				"topic", topic,
				"key", string(msg.Key),
				"partiton", msg.TopicPartition.Partition,
				"offset", msg.TopicPartition.Offset,
			)
			producerCnt.WithLabelValues(p.clientId, topic, "delievery", "success").Inc()
		}
	case <-time.After(time.Duration(p.cfg.Producer.SendTimeoutMs) * time.Millisecond):
		p.log.Error("Timeout waiting for message delivery",
			"topic", topic,
			"key", string(msg.Key),
			"timeout_ms", p.cfg.Producer.SendTimeoutMs)
		p.producer.Flush(p.cfg.Producer.SendTimeoutMs)
		return errors.New("timeout witing for message delivery")
	}

	return nil
}

func (p *producer) ProduceBatch(msgs []*kafka.Message) (err error) {
	for _, msg := range msgs {
		err := p.Produce(msg)
		if err != nil {
			return err
		}
	}
	p.log.Info("Async bathc produc completed", "count", len(msgs))
	return nil
}

func (p *producer) Stop() {
	p.mu.Lock()
	prod := p.producer
	p.mu.Unlock()
	if prod != nil {
		prod.Flush(p.cfg.Producer.SendTimeoutMs)
		prod.Close()
		p.deliveryWg.Wait()
		p.mu.Lock()
		p.producer = nil
		p.mu.Unlock()
		p.log.Info("Kafka producer stopped!")
	}
}
