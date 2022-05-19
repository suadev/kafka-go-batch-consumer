package config

var AppSettings Settings

type Settings struct {
	ProduceFakeData        bool   `env:"PRODUCE_FAKE_DATA" env-default:"true"`
	KafkaBrokerAddress     string `env:"KAFKA_BROKER_ADDRESS" env-default:"localhost:9092"`
	KafkaTopicName         string `env:"KAFKA_TOPIC_NAME" env-default:"product_events"`
	KafkaConsumerGroupName string `env:"KAFKA_CONSUMER_GROUP_NAME" env-default:"product_batch_consumer"`
	ElasticURL             string `env:"ELASTIC_URL" env-default:"http://localhost:9200"`
	ElasticIndexName       string `env:"ELASTIC_INDEX_NAME" env-default:"products"`
}
