package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/chanmaoganda/go-project-template/config"
	"github.com/chanmaoganda/go-project-template/services"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetFormatter(
		&logrus.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05",
			FullTimestamp:   true,
			ForceColors:     true,
		},
	)
	logrus.SetLevel(logrus.DebugLevel)
	sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)
}

func main() {
	settings, err := config.NewSettings()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}

	consume_topics := strings.Split(settings.Kafka.ConsumeTopic, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy := services.NewConsumerGroupProxy(settings.Kafka)
	ch := proxy.MessageChan()

	go proxy.StartConsume(ctx, consume_topics)

	for msg := range ch {
		logrus.Debug(string(msg.Value))
	}
}
