package model

import (
	"context"
	"errors"
	"strings"

	"github.com/IBM/sarama"
	"github.com/chanmaoganda/go-project-template/config"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	MessageChan chan *sarama.ConsumerMessage
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
			c.MessageChan <- message
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func NewConsumer(kafkaCfg *config.KafkaConfig) *Consumer {
	return &Consumer{
		MessageChan: make(chan *sarama.ConsumerMessage, kafkaCfg.ConsumerMessageBufferSize),
	}
}

type ConsumerGroupProxy struct {
	consumer *Consumer
	group sarama.ConsumerGroup
}

func (p *ConsumerGroupProxy) MessageChan() <- chan *sarama.ConsumerMessage{
	return p.consumer.MessageChan
}

func NewConsumerGroupProxy(kafkaCfg *config.KafkaConfig) *ConsumerGroupProxy {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0

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

	group, err := sarama.NewConsumerGroup(strings.Split(kafkaCfg.BrokerAddress, ","), kafkaCfg.ConsumerGroup, config)
	if err != nil {
		logrus.Errorf("Error creating consumer group group: %v", err)
	}

	return &ConsumerGroupProxy{
		group: group,
		consumer: NewConsumer(kafkaCfg),
	}
}

func (p *ConsumerGroupProxy) Consume(ctx context.Context, topics []string) {
	go func() {
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
	} ()
}
