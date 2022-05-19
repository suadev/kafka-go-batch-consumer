package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-go-batch-consumer/config"
	"strconv"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type ElasticClient struct {
	indexName string
	client    *elasticsearch.Client
}

var indexConfig = `{
	"settings":{
	    "max_result_window": 10000,
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
			"properties": {
				"id": {
					"type": "integer"
				},
				"name": {
					"type": "text"
				}			
			}
	}
}`

func NewElasticClient() (*ElasticClient, error) {
	conf := config.AppSettings
	cfg := elasticsearch.Config{
		Addresses: []string{
			conf.ElasticURL,
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	c := ElasticClient{
		indexName: conf.ElasticIndexName,
		client:    client,
	}
	return &c, nil
}

func (c *ElasticClient) BulkIndexDocuments(docs []ElasticProductModel) error {
	if len(docs) < 1 {
		return errors.New("[Elastic] At least one doc is required for batch indexing.")

	}
	bulkRequests, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: c.client,
	})

	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			fmt.Printf("Cannot encode %v", doc.ID)
			continue
		}

		bulkRequests.Add(context.Background(),
			esutil.BulkIndexerItem{
				Index:      c.indexName,
				Action:     "index",
				DocumentID: strconv.Itoa(doc.ID),
				Body:       bytes.NewReader(data),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						fmt.Printf("%v", err)
					} else {
						fmt.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
	}

	if err := bulkRequests.Close(context.Background()); err != nil {
		fmt.Printf("%v", err)
		return err
	}
	fmt.Printf("[Elastic] Indexed %d documents.\n", len(docs))
	return nil
}
