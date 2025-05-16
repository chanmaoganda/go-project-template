package main

import (
	"context"
	"os"
	"strings"

	"github.com/chanmaoganda/go-project-template/model"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetFormatter(
		&logrus.TextFormatter{
			TimestampFormat : "2006-01-02 15:04:05",
			FullTimestamp:true,
			ForceColors: true,
		},
	)
	logrus.SetLevel(logrus.DebugLevel)
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
	topics := strings.Split(worker.Kafka.ConsumeTopic, ",")
	ch := proxy.MessageChan()
	
	proxy.Consume(ctx, topics)
	for msg := range ch {
		logrus.Debug(string(msg.Value))
	}
}
