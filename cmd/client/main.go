package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/chanmaoganda/go-project-template/common"
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
	worker, err := services.NewWorker()
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxy := services.NewProducerProxy(worker.Kafka)
	ch := proxy.MessageChan()

	go func() {
		messages := []int {
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		}
		logrus.Debug("Sending Messages")
		for _, msg := range messages {
			str_msg := strconv.Itoa(msg)
			ch <- common.NewProducerMessage(worker.Kafka.ProduceTopic, str_msg, []byte(str_msg))
		}
		cancel()
	} ()

	go proxy.StartProduce(ctx)

	<- ctx.Done()
	time.Sleep(2 * time.Second)
}
