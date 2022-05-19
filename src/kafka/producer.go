package kafka

import (
	"encoding/json"
	"fmt"
	"kafka-go-batch-consumer/config"
	"kafka-go-batch-consumer/product"
	"log"

	"github.com/Shopify/sarama"
	"github.com/brianvoe/gofakeit/v6"
)

type Producer struct {
	asyncProducer sarama.AsyncProducer
}

func NewProducer() (*Producer, error) {

	cnf := sarama.NewConfig()

	admin, err := sarama.NewClusterAdmin(
		[]string{config.AppSettings.KafkaBrokerAddress},
		cnf)
	if err != nil {
		log.Fatal("Error while creating cluster admin: ", err.Error())
	}
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(config.AppSettings.KafkaTopicName, &sarama.TopicDetail{
		NumPartitions:     5,
		ReplicationFactor: 1,
	}, false)
	if err != nil {
		fmt.Println(err.Error())
	}

	producer, err := sarama.NewAsyncProducer(
		[]string{config.AppSettings.KafkaBrokerAddress},
		cnf)

	if err != nil {
		return nil, err
	}
	return &Producer{
		asyncProducer: producer,
	}, nil
}

func (p *Producer) StartProduce(done chan struct{}) {
	messageCount := 5000000
	fmt.Printf("[Producer] Started event producing. Total Count: %d \n", messageCount)

	for i := 0; i < messageCount; i++ {
		msg := product.ProductChangedEvent{
			ID:         i,
			Name:       gofakeit.Fruit(),
			CategoryID: i % 40,
		}
		msgBytes, err := json.Marshal(msg)
		keyBytes, err := json.Marshal(i)
		if err != nil {
			continue
		}
		select {
		case <-done:
			return
		case p.asyncProducer.Input() <- &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(keyBytes),
			Topic: config.AppSettings.KafkaTopicName,
			Value: sarama.ByteEncoder(msgBytes),
		}:
		case err := <-p.asyncProducer.Errors():
			fmt.Printf("[Producer] Failed to send message to kafka, err: %s, msg: %s\n", err, msgBytes)
		}
	}
	fmt.Printf("[Producer] Finished %d events producing... \n", messageCount)
}

func (p *Producer) Close() error {
	if p != nil {
		return p.asyncProducer.Close()
	}
	return nil
}
