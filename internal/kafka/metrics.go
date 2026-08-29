package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	consumerCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_operations",
		Help: "Operations in kafka consumer",
	}, []string{"name", "operation", "status"})

	consumerHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kafka_consumer_duration_seconds",
		Help: "Duration of kafka consumer opartions",
	}, []string{"name", "operation"})

	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Kafka consumer lag by topic/partiton/group",
	}, []string{"topic", "partiton", "group"})

	consumerAssigenedPartitions = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_assigned_partitons",
		Help: "Number of partitons assigned to consumer by topic/group",
	}, []string{"topic", "group"})

	producerCnt = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_producer_operations",
		Help: "Operations in kafka producer",
	}, []string{"client_id", "topic", "operation", "status"})

	producerHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kafka_producer_duration_seconds",
		Help: "Duration of kafka producer operations",
	}, []string{"client_id", "topic", "operation"})
)
