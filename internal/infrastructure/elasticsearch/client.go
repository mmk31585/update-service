package elasticsearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type Client struct {
	client *elasticsearch.Client
	index  string
}

func NewClient(esURL, username, password, index string) (*Client, error) {
	u, err := url.Parse(esURL)
	if err != nil {
		return nil, fmt.Errorf("invalid elasticsearch URL: %w", err)
	}

	cfg := elasticsearch.Config{
		Addresses: []string{u.String()},
		Username:  username,
		Password:  password,
	}

	if u.Scheme == "http" || u.Scheme == "https" {
		cfg.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating elasticsearch client: %w", err)
	}

	res, err := es.Info()
	if err != nil {
		return nil, fmt.Errorf("error connecting to elasticsearch: %w", err)
	}
	res.Body.Close()
	log.Printf("connected to elasticsearch: %s", u.String())

	return &Client{client: es, index: index}, nil
}

func (c *Client) Index(ctx context.Context, doc []byte, id string) error {
	req := esapi.IndexRequest{
		Index:      c.index,
		DocumentID: id,
		Body:       io.NopCloser(bytes.NewReader(doc)),
		Refresh:    "false",
	}

	res, err := req.Do(ctx, c.client)
	if err != nil {
		return fmt.Errorf("error indexing document: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return fmt.Errorf("elasticsearch returned status %d", res.StatusCode)
	}
	return nil
}
