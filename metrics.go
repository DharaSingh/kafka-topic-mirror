package main

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type Metrics struct {
	registerer            sync.Once
	consumedMessagesCount *prometheus.CounterVec
	producedMessagesCount *prometheus.CounterVec
}

var metrics = Metrics{
	registerer: sync.Once{},
}

func GetMetrics() *Metrics {
	return &metrics
}

const appName = "kafka_topic_mirror"
const labelApp = "app"

func InitMetrics(promServerAddr string) (s *Metrics) {
	metrics.consumedMessagesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumed_messages_count",
			Help: "Number of consumed messages",
		},
		[]string{labelApp, "topic_name"},
	)

	metrics.producedMessagesCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "produced_messages_count",
			Help: "Number of produced messages",
		},
		[]string{labelApp, "topic_name"},
	)

	metrics.registerMetrics()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(promServerAddr, nil)
		if err != nil {
			log.Error(err)
		}
	}()

	return
}

func (s *Metrics) registerMetrics() {
	s.registerer.Do(func() {
		prometheus.MustRegister(metrics.consumedMessagesCount)
		prometheus.MustRegister(metrics.producedMessagesCount)
	})
}

func (s *Metrics) ConsumedMessagesCountInc(topicName string) {
	s.consumedMessagesCount.WithLabelValues(appName, topicName).Inc()
}

func (s *Metrics) AddProducedMessagesCount(topicName string, count int) {
	s.producedMessagesCount.WithLabelValues(appName, topicName).Add(float64(count))
}
