package config

type Settings struct {
	Kafka *KafkaConfig `yaml:"kafka"`
	Redis *RedisConfig `yaml:"redis"`
}

type KafkaConfig struct {
	BrokerAddress string `yaml:"broker_address"`
	ConsumerGroup string `yaml:"consumer_group"`
	ConsumeTopic string `yaml:"consume_topic"`
	ConsumerMessageBufferSize int `yaml:"consumer_message_buffer_size"`
	ProduceTopic string `yaml:"produce_topic"`
	ProducerMessageBufferSize int `yaml:"producer_message_buffer_size"`
	Assignor string `yaml:"assignor"`
	OffsetInitial string `yaml:"offset_initial"`
}

type RedisConfig struct {
	Address string `yaml:"address"`
}
