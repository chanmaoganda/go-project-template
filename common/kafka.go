package common

import "github.com/IBM/sarama"

func NewProducerMessage(topic string, key string, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
