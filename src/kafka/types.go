package kafka

import "kafka-go-batch-consumer/elastic"

type ProductChangedEvent struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	CategoryID int    `json:"categoryId"`
}

func (l ProductChangedEvent) ToElasticDocument() elastic.ElasticProductModel {
	return elastic.ElasticProductModel{
		ID:   l.ID,
		Name: l.Name,
	}
}
