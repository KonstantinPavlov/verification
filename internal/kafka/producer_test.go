package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KonstantinPavlov/verification/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Тест успешного создания и работы продюсера через MockCluster
func TestProducer_Success_Workflow(t *testing.T) {
	// 1. Создаем встроенный Mock-кластер Kafka
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %s", err)
	}
	defer mockCluster.Close()

	// 2. Подготавливаем конфигурацию, указывая адрес mock-кластера
	cfg := DefaultKafkaAppConfig()
	cfg.Brokers = mockCluster.BootstrapServers()
	cfg.Producer.ClientId = "test-client"
	cfg.Producer.Acks = 1
	cfg.Producer.SendTimeoutMs = 2000

	// Создаем логгер (используем дефолтный из slog для теста)
	testLog := logger.New()

	// 3. Инициализируем наш продюсер
	p := NewProducer(&cfg, testLog)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Стартуем продюсер (запускаются горутины чтения логов и отчетов)
	err = p.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start producer: %s", err)
	}
	defer p.Stop()

	topic := "test-topic"

	// --- Тест метода Produce (Асинхронный) ---
	t.Run("Produce Async", func(t *testing.T) {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("key-async"),
			Value:          []byte("hello async"),
		}

		err := p.Produce(msg)
		if err != nil {
			t.Errorf("Produce failed: %s", err)
		}
	})

	// --- Тест метода ProduceAndGet (Синхронный с доставкой) ---
	t.Run("ProduceAndGet Sync", func(t *testing.T) {
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("key-sync"),
			Value:          []byte("hello sync"),
		}

		// Выделяем контекст с таймаутом на случай зависания
		testCtx, testCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer testCancel()

		err := p.ProduceAndGet(testCtx, msg)
		if err != nil {
			t.Errorf("ProduceAndGet failed: %s", err)
		}
	})
}

func TestProducer_Uninitialized(t *testing.T) {
	cfg := &KafkaAppConfig{}
	testLog := logger.New()
	p := NewProducer(cfg, testLog)

	topic := "test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
	}

	// Проверяем обработку ошибки, если Start() не вызывался (p.producer == nil)
	err := p.Produce(msg)
	if err == nil || err.Error() != "Producer not initialized!" {
		t.Errorf("Expected 'Producer not initialized!' error, got: %v", err)
	}
}

func TestProducer_ProduceBatch(t *testing.T) {
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %s", err)
	}
	defer mockCluster.Close()

	cfg := &KafkaAppConfig{Brokers: mockCluster.BootstrapServers()}
	testLog := logger.New()
	p := NewProducer(cfg, testLog).(*producer) // приводим к структуре для теста

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Failed to start: %s", err)
	}
	defer p.Stop()

	topic := "batch-topic"
	msgs := []*kafka.Message{
		{TopicPartition: kafka.TopicPartition{Topic: &topic}, Key: []byte("k1"), Value: []byte("v1")},
		{TopicPartition: kafka.TopicPartition{Topic: &topic}, Key: []byte("k2"), Value: []byte("v2")},
	}

	// Проверяем успешную отправку батча
	err = p.ProduceBatch(msgs)
	if err != nil {
		t.Errorf("ProduceBatch failed: %s", err)
	}
}

// --- Тест ошибок и контекста в ProduceAndGet ---
func TestProducer_ProduceAndGet_EdgeCases(t *testing.T) {
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create mock cluster: %s", err)
	}
	defer mockCluster.Close()

	cfg := DefaultKafkaAppConfig()
	cfg.Brokers = mockCluster.BootstrapServers()
	cfg.Producer.ClientId = "test-client"
	cfg.Producer.Acks = 1
	cfg.Producer.SendTimeoutMs = 100

	testLog := logger.New()
	p := NewProducer(&cfg, testLog)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = p.Start(ctx)
	defer p.Stop()

	// Сценарий 1: Контекст отменен ДО или ВО ВРЕМЯ ожидания ответа
	t.Run("Context Canceled", func(t *testing.T) {
		canceledCtx, cancelNow := context.WithCancel(context.Background())
		cancelNow() // Отменяем контекст мгновенно

		topic := "context-topic"
		msg := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic}}
		err := p.ProduceAndGet(canceledCtx, msg)
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	})

	// Сценарий 2: Ошибка доставки (Имитируем через отключение брокера)
	t.Run("Delivery Error Via Broker Down", func(t *testing.T) {
		// "Выключаем" единственный брокер в нашем мок-кластере
		err := mockCluster.SetBrokerDown(1) // ID брокера в MockCluster начинается с 1
		if err != nil {
			t.Fatalf("Failed to set broker down: %s", err)
		}

		topic := "failed-delivery-topic"
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("err-key"),
			Value:          []byte("err-value"),
		}

		// Запускаем отправку. Так как брокер лежит, мы гарантированно зайдем
		// либо в ветку ошибки доставки, либо сработает таймаут (time.After),
		// что поднимет покрытие ProduceAndGet до максимума.
		err = p.ProduceAndGet(context.Background(), msg)
		if err == nil {
			t.Error("Expected error due to dead broker, but got nil")
		}
	})
}
