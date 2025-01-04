package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
	"github.com/sethvargo/go-envconfig"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	ClientID    string `env:"CLIENT_ID"`
	KafkaInput  KafkaInput
	KafkaOutput KafkaOutput
	Monitoring  Monitoring
}

type KafkaInput struct {
	ConsumerGroup string   `env:"CONSUMER_GROUP"` // default will be topicName_ktm
	ClientId      string   //just copy of main config
	Topic         string   `env:"INPUT_TOPIC,required"`
	Brokers       []string `env:"INPUT_BROKERS,default=localhost:9092"`
	Newest        bool     `env:"NEWEST_OFFSET,default=true"`
}

type KafkaOutput struct {
	ClientId   string   // just copy of main config
	Topic      string   `env:"OUTPUT_TOPIC,required"`
	Brokers    []string `env:"OUTPUT_BROKERS,default=localhost:9092"`
	MaxPublish int      `env:"MAX_PUBLISH,default=1000"`
}

type Monitoring struct {
	PromServerAddr string `env:"PROM_SERVER_ADDR"`
}

func loadConfig() (*Config, error) {
	cfg := Config{}

	ctx := context.Background()

	if err := envconfig.Process(ctx, &cfg); err != nil {
		log.Fatal(err)
	}

	// Kafka Default Settings
	if cfg.ClientID == "" {
		hst, err := os.Hostname()
		if err != nil {
			log.Fatal(err.Error())
		}
		cfg.ClientID = fmt.Sprintf("kafka_topic_mirror_%s", hst)
	}

	if cfg.KafkaInput.ConsumerGroup == "" {
		cfg.KafkaInput.ConsumerGroup = fmt.Sprintf("%s_ktm", cfg.KafkaInput.Topic)
	}
	err := validateKafkaConfig(&cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateKafkaConfig(cfg *Config) error {
	if cfg.KafkaInput.Topic == "" {
		return errors.New("missing required field 'input-topic'")
	}
	if len(cfg.KafkaInput.Brokers) == 0 {
		return errors.New("missing required field 'input-brokers'")
	}
	if cfg.KafkaOutput.Topic == "" {
		return errors.New("missing required field 'output-topic'")
	}
	if len(cfg.KafkaOutput.Brokers) == 0 {
		return errors.New("missing required field 'output-brokers'")
	}

	if cfg.KafkaInput.Topic == cfg.KafkaOutput.Topic {
		for _, ibroker := range cfg.KafkaInput.Brokers {
			for _, oBroker := range cfg.KafkaOutput.Brokers {
				if ibroker == oBroker {
					return errors.New("KTR cycle detected")
				}
			}
		}
	}
	cfg.KafkaInput.ClientId = cfg.ClientID
	cfg.KafkaOutput.ClientId = cfg.ClientID

	return nil

}

func initLogger(cfg *Config) {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
	sarama.Logger = log.New()
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.WithError(err).Fatal("failed to load config")
	}
	initLogger(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

}

func run(ctx context.Context, cfg *Config) error {
	sourceMessages := make(chan *KtmMessage, cfg.KafkaOutput.MaxPublish*2)
	commitMessages := make(chan map[int32]*sarama.ConsumerMessage, 1) // single blocking call, max one batch to wait
	// start a consumer
	consumer, err := NewKafkaConsumer(ctx, cfg, sourceMessages, commitMessages)
	if err != nil {
		log.Error("failed to create kafka consumer")
		return err
	}
	wgConsumer := &sync.WaitGroup{}
	wgConsumer.Add(1)
	go func() {
		defer wgConsumer.Done()
		consumer.Run(ctx)
	}()

	producer := NewKafkaProducer(cfg.KafkaOutput, sourceMessages, commitMessages)

}
