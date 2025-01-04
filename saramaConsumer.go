package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// Consumer interface for shared_lib kafka consumer implementations
type Consumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	MarkMessage(*sarama.ConsumerMessage, string)
	Close() error
}

// ConsumerConfig required to create a new instance of a consumer
type ConsumerConfig struct {
	ClientID      string
	Brokers       []string
	Topic         string
	ConsumerGroup string
	NewestOffset  bool
	SaramaConfig  *sarama.Config
}

// DefaultConsumer represents a Sarama consumer group consumer
type DefaultConsumer struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    ConsumerConfig

	closeOnce sync.Once
	ready     chan bool
	messages  chan *sarama.ConsumerMessage
	session   struct {
		sess sarama.ConsumerGroupSession
		mu   sync.Mutex
	}
}

// NewConsumer returns a instance of DefaultConsumer
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (Consumer, error) {
	c := &DefaultConsumer{}
	c.cfg = cfg
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.messages = make(chan *sarama.ConsumerMessage)
	err := c.cfg.update()
	if err != nil {
		return nil, err
	}

	err = c.start()
	if err != nil {
		return nil, err
	}

	<-c.ready

	return c, err
}

func (cfg *ConsumerConfig) update() error {
	cfg.SaramaConfig = sarama.NewConfig()
	if cfg.NewestOffset {
		cfg.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		cfg.SaramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	cfg.SaramaConfig.ClientID = cfg.ClientID
	return nil
}

func (c *DefaultConsumer) start() (err error) {
	client, err := sarama.NewConsumerGroup(c.cfg.Brokers, c.cfg.ConsumerGroup, c.cfg.SaramaConfig)
	if err != nil {
		return errors.Wrap(err, "error creating consumer group client")
	}

	c.ready = make(chan bool)
	go c.loop(client)

	return
}

func (c *DefaultConsumer) loop(client sarama.ConsumerGroup) {
	defer func() {
		c.Close()
		err := client.Close()
		if err != nil {
			fmt.Print(errors.Wrap(err, "error while closing client"))
		}
	}()

	for {
		fmt.Print("new client.Consume iteration")
		if err := client.Consume(c.ctx, []string{c.cfg.Topic}, c); err != nil {
			sarama.Logger.Println(errors.Wrap(err, "consume fail"))
			if strings.Contains(err.Error(), "context canceled") {
				return
			}
			time.Sleep(time.Second * 10)
		}

		if c.ctx.Err() != nil {
			return
		}

		c.ready = make(chan bool)
	}
}

// MarkMessage marks a message as consumed.
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
func (c *DefaultConsumer) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	if msg.Topic == "" {
		return
	}
	if c.session.sess == nil {
		sarama.Logger.Printf("not able to mark message offset, partition: %d, offset: %d", msg.Partition, msg.Offset)
		return
	}

	// mark on kafka brokers
	c.session.mu.Lock()
	c.session.sess.MarkMessage(msg, metadata)
	c.session.mu.Unlock()

}

// Messages returns the read channel for the messages fetched with ConsumeClaim
func (c *DefaultConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

// Close safely closes the consumer and releases all resources
func (c *DefaultConsumer) Close() error {
	c.cancel()
	time.Sleep(time.Second * 1) // let the cancel event take effect - not sure though
	go func() {
		time.Sleep(time.Second * 5)
		c.closeOnce.Do(func() { close(c.messages) })
	}()

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *DefaultConsumer) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("setup of new session generation: %+v; member id: %+v", session.GenerationID(), session.MemberID())
	close(c.ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *DefaultConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// session is for every partition consume, can be shared among partitions
func (c *DefaultConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.session.mu.Lock()
	c.session.sess = session
	c.session.mu.Unlock()
loop:
	for {
		select {
		case <-c.ctx.Done():
			fmt.Printf("Client Context Done, break ConsumeClaim")
			break loop
		case <-session.Context().Done():
			fmt.Printf("ConsumerGroupSession Done, break ConsumeClaim")
			break loop
		case msg, ok := <-claim.Messages():
			if !ok {
				fmt.Printf("claim message channel closed, break ConsumeClaim")
				break loop
			}

			select {
			case <-session.Context().Done():
				fmt.Printf("ConsumerGroupSession Done, break ConsumeClaim")
				break loop
			case <-c.ctx.Done():
				fmt.Printf("Client Context Done, break ConsumeClaim")
				break loop
				// Attempting to send a value into a closed channel will panic.
				// This code won't reach here due to the panic.
				// make sure service is canceling the context, waiting for some time and then closing the msg
			case c.messages <- msg:
				continue
			}

		}
	}

	c.session.mu.Lock()
	c.session.sess = nil
	c.session.mu.Unlock()

	if c.ctx.Err() != nil {
		c.closeOnce.Do(func() { close(c.messages) })
	}

	return nil
}
