package main

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	batchSize  int
	topic      string
	producer   sarama.SyncProducer
	inputMsgs  chan *KtmMessage
	commitMsgs chan map[int32]*sarama.ConsumerMessage
}

func NewKafkaProducer(output KafkaOutput, inputMsgs chan *KtmMessage, commitMsgs chan map[int32]*sarama.ConsumerMessage) (*KafkaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = output.ClientId
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Retry.Max = 3
	cfg.Producer.Return.Successes = true
	cfg.Producer.Compression = sarama.CompressionSnappy

	producer, err := sarama.NewSyncProducer(output.Brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{producer: producer, inputMsgs: inputMsgs, commitMsgs: commitMsgs, batchSize: output.MaxPublish, topic: output.Topic}, nil
}

func (p *KafkaProducer) Run() {
	m := GetMetrics()
	for {
		var (
			buffer         []*sarama.ProducerMessage
			commitMessages = make(map[int32]*sarama.ConsumerMessage)
			shutdown       bool
		)

	loop:
		for {
			select {
			case ktm, ok := <-p.inputMsgs:
				if !ok {
					shutdown = true
					break loop
				}
				buffer = append(buffer, ktm.out)

				// get the max offset from the partition for commit as we dont have to commit all the msgs
				// just max is enough
				if _, ok := commitMessages[ktm.in.Partition]; !ok {
					commitMessages[ktm.in.Partition] = ktm.in
				}
				if commitMessages[ktm.in.Partition].Offset < ktm.in.Offset {
					commitMessages[ktm.in.Partition] = ktm.in
				}

				if len(buffer) >= p.batchSize {
					break loop
				}
			case <-time.After(time.Millisecond * 100):
				break loop
			}
		}
		// send the batch
		if len(buffer) > 0 {
			err := p.producer.SendMessages(buffer)
			if err != nil {
				log.Printf("Failed to send messages, stopping the service : %s", err)
				return
			}
			m.AddProducedMessagesCount(p.topic, len(buffer))
			// send for commit
			p.commitMsgs <- commitMessages
			buffer = buffer[:0]
		}

		if shutdown {
			return
		}
	}
}
