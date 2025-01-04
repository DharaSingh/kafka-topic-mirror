package main

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

type KafkaConsumer struct {
	consumer    Consumer
	outputMsgs  chan *KtmMessage
	commitMsgs  chan map[int32]*sarama.ConsumerMessage
	outputTopic string // to create a copy of msg with topic name
}

func NewKafkaConsumer(ctx context.Context, cfg *Config, output chan *KtmMessage, commit chan map[int32]*sarama.ConsumerMessage) (*KafkaConsumer, error) {
	consumerCfg := ConsumerConfig{
		ClientID:      cfg.KafkaInput.ClientId,
		Brokers:       cfg.KafkaInput.Brokers,
		ConsumerGroup: cfg.KafkaInput.ConsumerGroup,
		Topic:         cfg.KafkaInput.Topic,
		NewestOffset:  cfg.KafkaInput.Newest,
	}
	consumer, err := NewConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, err
	}
	kc := &KafkaConsumer{
		consumer:    consumer,
		outputMsgs:  output,
		commitMsgs:  commit,
		outputTopic: cfg.KafkaOutput.Topic,
	}
	return kc, nil
}

func (kc *KafkaConsumer) Run(ctx context.Context) {

	wgConsumer := &sync.WaitGroup{}
	wgConsumer.Add(1)
	go func() {
		defer wgConsumer.Done()
		kc.consume(ctx)
	}()

	wgCommit := &sync.WaitGroup{}
	wgCommit.Add(1)
	go func() {
		defer wgCommit.Done()
		kc.commit()
	}()

	wgConsumer.Wait()
	close(kc.outputMsgs)
	wgCommit.Wait()

}

func (kc *KafkaConsumer) consume(ctx context.Context) {
	m := GetMetrics()
	for {
		select {
		case <-ctx.Done():
			log.Warn("Shutting down consumer")
			return
		case msg, ok := <-kc.consumer.Messages():
			if !ok {
				log.Warn("unexpected channel close from kafka, re-join consumer")
				return
			}
			kc.outputMsgs <- copyMessage(msg, kc.outputTopic)
			m.ConsumedMessagesCountInc(msg.Topic)
		}
	}
}

func (kc *KafkaConsumer) commit() {
	for partitionOffsets := range kc.commitMsgs {
		for _, v := range partitionOffsets {
			kc.consumer.MarkMessage(v, "")
		}
	}
}

type KtmMessage struct {
	in  *sarama.ConsumerMessage
	out *sarama.ProducerMessage
}

func copyMessage(from *sarama.ConsumerMessage, topic string) *KtmMessage {
	to := sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(from.Value),
		Key:       sarama.ByteEncoder(from.Key),
		Timestamp: from.Timestamp,
		Headers:   copyHeaders(from.Headers),
	}

	ktm := KtmMessage{
		in:  from,
		out: &to,
	}
	return &ktm
}

func copyHeaders(headers []*sarama.RecordHeader) []sarama.RecordHeader {
	var to []sarama.RecordHeader
	for _, header := range headers {
		to = append(to, *header)
	}
	return to
}
