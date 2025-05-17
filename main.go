package main

import (
	"context"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/chanmaoganda/go-project-template/model"
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
	worker, err := model.NewWorker()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy := model.NewConsumerGroupProxy(worker.Kafka)
	ch := proxy.MessageChan()

	go proxy.StartConsume(ctx)
	for msg := range ch {
		logrus.Debug(string(msg.Value))
	}
}
