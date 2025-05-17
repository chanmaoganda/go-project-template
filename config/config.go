package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Settings struct {
	Kafka *KafkaConfig `yaml:"kafka"`
	Redis *RedisConfig `yaml:"redis"`
}

type KafkaConfig struct {
	BrokerAddress             string `yaml:"broker_address"`
	ConsumerGroup             string `yaml:"consumer_group"`
	ConsumeTopic              string `yaml:"consume_topic"`
	ConsumerMessageBufferSize int    `yaml:"consumer_message_buffer_size"`
	ProduceTopic              string `yaml:"produce_topic"`
	ProducerMessageBufferSize int    `yaml:"producer_message_buffer_size"`
	Assignor                  string `yaml:"assignor"`
	OffsetInitial             string `yaml:"offset_initial"`
}

type RedisConfig struct {
	Address string `yaml:"address"`
}

func NewSettings() (*Settings, error) {
	yamlFile, err := os.ReadFile("settings.yml")
	if err != nil {
		return nil, err
	}

	var settings Settings

	err = yaml.Unmarshal(yamlFile, &settings)
	if err != nil {
		return nil, err
	}
	
	return &settings, nil
}