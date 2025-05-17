package services

import (
	"context"
	"errors"
	"strings"

	"github.com/IBM/sarama"
	"github.com/chanmaoganda/go-project-template/config"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	ConsumerChan chan *sarama.ConsumerMessage
	// you can add more fields here when parsing messages
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				logrus.Printf("message channel was closed")
				return nil
			}
			// use chan to split logics from consuming to business logics
			c.ConsumerChan <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func NewConsumer(kafkaCfg *config.KafkaConfig) *Consumer {
	return &Consumer{
		ConsumerChan: make(chan *sarama.ConsumerMessage, kafkaCfg.ConsumerMessageBufferSize),
	}
}

func NewSaramaConfig(kafkaCfg *config.KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0

	// consumer configurations
	{
		switch kafkaCfg.Assignor {
		case "sticky":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		case "roundrobin":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
		case "range":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		default:
			logrus.Debugf("Unrecognized consumer group partition assignor: %s", kafkaCfg.Assignor)
		}

		if kafkaCfg.OffsetInitial == "newest" {
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		}
		logrus.Debug(kafkaCfg.Assignor)
	}

	// producer configurations
	{
		config.Producer.RequiredAcks = sarama.WaitForLocal
		config.Producer.Return.Successes = true
		config.Producer.Compression = sarama.CompressionGZIP
		// config.Producer.Flush.Frequency = 500 * time.Millisecond
	}

	return config
}

type ConsumerGroupProxy struct {
	consumer *Consumer
	group    sarama.ConsumerGroup
}

func (p *ConsumerGroupProxy) MessageChan() <-chan *sarama.ConsumerMessage {
	return p.consumer.ConsumerChan
}

// Warning! before consuming, remember to get the chan from MessageChan(), messages would be sent to this chan
//
// This function is a non-blocking consume
func (p *ConsumerGroupProxy) StartConsume(ctx context.Context, topics []string) {
	logrus.Debug("Consuming started")
	for {
		if err := p.group.Consume(ctx, topics, p.consumer); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			logrus.Errorf("Error from consumer: %v", err)
		}

		// check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
	}
}

func NewConsumerGroupProxy(kafkaCfg *config.KafkaConfig) *ConsumerGroupProxy {
	config := NewSaramaConfig(kafkaCfg)

	group, err := sarama.NewConsumerGroup(strings.Split(kafkaCfg.BrokerAddress, ","), kafkaCfg.ConsumerGroup, config)
	if err != nil {
		logrus.Panicf("Error creating consumer group group: %v", err)
	}

	return &ConsumerGroupProxy{
		group:    group,
		consumer: NewConsumer(kafkaCfg),
	}
}

type ProducerProxy struct {
	Producer     sarama.SyncProducer
	ProducerChan chan *sarama.ProducerMessage
}

func (p *ProducerProxy) StartProduce(ctx context.Context) {
	for msg := range p.ProducerChan {
		partition, offset, err := p.Producer.SendMessage(msg)
		if err != nil {
			logrus.Errorf("Failed to send message. %v", err)
		} else {
			logrus.Debugf("Message sent successfully to partition %d at offset %d\n", partition, offset)
		}
	}
}

func (p *ProducerProxy) MessageChan() chan<- *sarama.ProducerMessage {
	return p.ProducerChan
}

func NewProducerProxy(kafkaCfg *config.KafkaConfig) *ProducerProxy {
	config := NewSaramaConfig(kafkaCfg)

	producer, err := sarama.NewSyncProducer(strings.Split(kafkaCfg.BrokerAddress, ","), config)
	if err != nil {
		logrus.Panicf("Error creating consumer group group: %v", err)
	}

	return &ProducerProxy{
		Producer:     producer,
		ProducerChan: make(chan *sarama.ProducerMessage, kafkaCfg.ProducerMessageBufferSize),
	}
}
