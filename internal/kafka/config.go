package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"math"
)

type KafkaAppConfig struct {
	Brokers  string `yaml:"brokers"`
	Consumer struct {
		GroupId              string   `yaml:"group-id"`
		ClientId             string   `yaml:"client-id"`
		Topics               []string `yaml:"topics"`
		PollTimeoutMs        int      `yaml:"poll-timeout-ms"`
		MaxPollIntervalMs    int      `yaml:"max-poll-interval-ms"`
		SessionTimeoutMs     int      `yaml:"session-timeout-ms"`
		AutoOffsetReset      string   `yaml:"auto-offset-reset"`
		BathcConsumer        bool     `yaml:"batch-consumer"`
		BatchSize            int      `yaml:"batch-size"`
		BathPollMaxWaitMs    int      `yaml:"batch-poll-max-wait-ms"`
		EnableRebalaceEvents bool     `yaml:"enable-rebalace-events"`
		FetchMinBytes        int      `yaml:"fetch-min-bytes"`
		FetchWaitMaxMs       int      `yaml:"fetch-wait-max-ms"`
		MaxAttempts          int      `yaml:"max-attempts"`
		Debug                string   `yaml:"debug"`
	} `yaml:"consumer"`

	Producer struct {
		Acks          int    `yaml:"acks"`
		ClientId      string `yaml:"client-id"`
		Retries       int    `yaml:"retries"`
		SendTimeoutMs int    `yaml:"send-timeout-ms"`
	} `yaml:"producer"`

	Tls struct {
		Enabled       bool   `yaml:"enabled"`
		CaFilePath    string `yaml:"ca-filepath"`
		CertFilePath  string `yaml:"cert-filepath"`
		KeyFilePath   string `yaml:"key-filepath"`
		SkipTlsVerify bool   `yaml:"skip-tls-verfy"`
	} `yaml:"tls"`
}

func DefaultKafkaAppConfig() KafkaAppConfig {
	cfg := KafkaAppConfig{
		Brokers: "",
	}
	cfg.Consumer.GroupId = ""
	cfg.Consumer.ClientId = ""
	cfg.Consumer.PollTimeoutMs = 500
	cfg.Consumer.MaxPollIntervalMs = 66000
	cfg.Consumer.SessionTimeoutMs = 60000
	cfg.Consumer.AutoOffsetReset = "earliest"
	cfg.Consumer.EnableRebalaceEvents = true
	cfg.Consumer.BathcConsumer = true
	cfg.Consumer.BatchSize = 10000
	cfg.Consumer.BathPollMaxWaitMs = 500
	cfg.Consumer.MaxAttempts = math.MaxInt / 2
	cfg.Consumer.Debug = ""
	cfg.Consumer.FetchMinBytes = 1
	cfg.Consumer.FetchWaitMaxMs = 500
	cfg.Producer.Acks = 1
	cfg.Producer.Retries = 3
	cfg.Producer.SendTimeoutMs = 30000
	cfg.Producer.ClientId = "kafka-producer"
	return cfg
}

type Message struct {
	*kafka.Message
	remainingAttempts uint64
}

type consumerOptions struct {
	Name      string
	OnMessage func(ctx context.Context, msg *Message) (needCommit bool)
	OnBatch   func(ctx context.Context, msgg []*Message) (needCommit bool)
}

func defaultConsumerOptions() *consumerOptions {
	return &consumerOptions{
		Name:      "default-consumer",
		OnMessage: func(ctx context.Context, msg *Message) (needCommit bool) { return false },
		OnBatch:   func(ctx context.Context, msgg []*Message) (needCommit bool) { return false },
	}
}

type OptConsumerSetter func(*consumerOptions)

func ConsumerNameOpt(name string) func(*consumerOptions) {
	return func(co *consumerOptions) {
		co.Name = name
	}
}

func ConsumerOnMessageOpt(onMessage func(ctx context.Context, msg *Message) (needCommit bool)) func(*consumerOptions) {
	return func(co *consumerOptions) {
		co.OnMessage = onMessage
	}
}

func ConsumerOnBatchOpt(OnBatch func(ctx context.Context, msgs []*Message) (needCommit bool)) func(*consumerOptions) {
	return func(co *consumerOptions) {
		co.OnBatch = OnBatch
	}
}
