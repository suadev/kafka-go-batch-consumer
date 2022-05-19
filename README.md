If you have many(?) Kafka messages that you need to take a full snapshot of and store in another database, this case study may be a good starting point for you.

When you start the application, it will produce 5M messages to Kafka with 5 partitions. 

After that, it will start a consumer group that store messages into the buffer and flush them to be bulk indexed in Elasticsearch.

You can fine-tune the current batch size (20K) and make your own performance tests.

<img src="https://raw.githubusercontent.com/suadev/kafka-go-batch-consumer/main/consumer_logs.png" height="500" />

