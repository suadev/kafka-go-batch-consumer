package kafka

import (
	"context"
	"fmt"
	"kafka-go-batch-consumer/config"
	"kafka-go-batch-consumer/elastic"

	"github.com/Shopify/sarama"
)

func StartBatchConsumer(topicName string, groupName string) (*ConsumerGroup, error) {
	elasticClient, err := elastic.NewElasticClient()
	if err != nil {
		panic(err)
	}

	handler := NewBatchConsumerGroupHandler(&BatchConsumerConfig{
		MaxMessageSize:        20000,
		TickerIntervalSeconds: 60,
		Callback: func(messages []elastic.ElasticProductModel, session sarama.ConsumerGroupSession) error {
			go elasticClient.BulkIndexDocuments(messages)
			return nil
		},
	})

	consumer, err := NewConsumerGroup([]string{topicName}, groupName, handler)
	if err != nil {
		return nil, err
	}
	return consumer, nil

}

func NewConsumerGroup(topics []string, groupName string, handler ConsumerGroupHandler) (*ConsumerGroup, error) {
	ctx := context.Background()
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{config.AppSettings.KafkaBrokerAddress},
		groupName,
		saramaConfig)

	if err != nil {
		errorString := fmt.Sprintf("Error creating consumer group client: %v", err)
		panic(errorString)
	}
	go func() {
		for {
			err := consumerGroup.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					panic(err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()

	handler.WaitReady() // wait consumer has been set up
	return &ConsumerGroup{
		cg: consumerGroup,
	}, nil
}

func (c *ConsumerGroup) Close() error {
	return c.cg.Close()
}

type BatchConsumerConfig struct {
	MaxMessageSize        int
	TickerIntervalSeconds int
	Callback              func([]elastic.ElasticProductModel, sarama.ConsumerGroupSession) error
}

type ConsumerGroup struct {
	cg sarama.ConsumerGroup
}

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}
