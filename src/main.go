package main

import (
	"fmt"
	"kafka-go-batch-consumer/config"
	"kafka-go-batch-consumer/kafka"
	"os"
	"os/signal"
	"syscall"

	"github.com/ilyakaznacheev/cleanenv"
)

func main() {
	initAppSettings()
	produceSampleData()
	startBatchConsumer()
}

func initAppSettings() {
	err := cleanenv.ReadEnv(&config.AppSettings)
	if err != nil {
		panic(err)
	}
}

func produceSampleData() {
	if !config.AppSettings.ProduceFakeData {
		fmt.Println("Skipping sample data producing step...")
		return
	}
	var done = make(chan struct{})
	defer close(done)
	producer, err := kafka.NewProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()
	producer.StartProduce(done)
}

func startBatchConsumer() {
	consumer, err := kafka.StartBatchConsumer(
		config.AppSettings.KafkaTopicName,
		config.AppSettings.KafkaConsumerGroupName,
	)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	fmt.Println("Received closing signal...", <-c)
}
