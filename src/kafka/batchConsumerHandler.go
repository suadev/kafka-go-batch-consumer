package kafka

import (
	"encoding/json"
	"fmt"
	"kafka-go-batch-consumer/elastic"
	"kafka-go-batch-consumer/product"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type BatchConsumerConfig struct {
	MaxMessageSize        int
	TickerIntervalSeconds int
	Callback              func([]elastic.ElasticProductModel, sarama.ConsumerGroupSession) error
}

type batchConsumerGroupHandler struct {
	batchConsumerConfig *BatchConsumerConfig
	ready               chan bool
	// buffer
	ticker        *time.Ticker
	messageBuffer []elastic.ElasticProductModel
	// lock to protect buffer operation
	mu sync.RWMutex
	// callback
	callback func([]elastic.ElasticProductModel, sarama.ConsumerGroupSession) error
}

func NewBatchConsumerGroupHandler(batchConsumerConfig *BatchConsumerConfig) ConsumerGroupHandler {
	handler := batchConsumerGroupHandler{
		ready:    make(chan bool, 0),
		callback: batchConsumerConfig.Callback,
	}
	handler.messageBuffer = make([]elastic.ElasticProductModel, 0, batchConsumerConfig.MaxMessageSize)
	handler.batchConsumerConfig = batchConsumerConfig
	handler.ticker = time.NewTicker(time.Duration(batchConsumerConfig.TickerIntervalSeconds) * time.Second)
	return &handler
}

func (h *batchConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *batchConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *batchConsumerGroupHandler) WaitReady() {
	<-h.ready
	return
}

func (h *batchConsumerGroupHandler) Reset() {
	h.ready = make(chan bool, 0)
	return
}

func (h *batchConsumerGroupHandler) flushBuffer(session sarama.ConsumerGroupSession) {
	if len(h.messageBuffer) > 0 {
		fmt.Printf("[Consumer] Flushing %d kafka messages for elasic indexing. \n", len(h.messageBuffer))
		if err := h.callback(h.messageBuffer, session); err == nil {
			h.messageBuffer = make([]elastic.ElasticProductModel, 0, h.batchConsumerConfig.MaxMessageSize)
		}
	}
}

func (h *batchConsumerGroupHandler) insertMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var productChangedEvent product.ProductChangedEvent
	err := json.Unmarshal(msg.Value, &productChangedEvent)
	if err != nil {
		return err
	}
	elasticDoc := productChangedEvent.ToElasticDocument()

	h.messageBuffer = append(h.messageBuffer, elasticDoc)
	if len(h.messageBuffer) >= h.batchConsumerConfig.MaxMessageSize {
		h.flushBuffer(session)
	}
	return nil
}

func (h *batchConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("[Consumer] Consumer is starting for the partition ", claim.Partition())
	// start := time.Now() // todo

	// Do not move the code below to a goroutine. ConsumeClaim() is already called within a goroutine.
	// ConsumeClaim() gets called individually  for each partition.
	claimMessageChan := claim.Messages()
	for {
		select {
		case msg, ok := <-claimMessageChan:
			if ok {
				if err := h.insertMessage(msg, session); err == nil {
					session.MarkMessage(msg, "")
				}
			} else {
				return nil
			}
		case <-h.ticker.C:
			h.safelyFlushBuffer(session)
		}
	}
}

func (h *batchConsumerGroupHandler) safelyFlushBuffer(session sarama.ConsumerGroupSession) {
	h.mu.Lock()
	h.flushBuffer(session)
	h.mu.Unlock()
}

type ConsumerGroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}
