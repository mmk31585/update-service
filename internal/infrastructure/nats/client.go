package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"update/internal/config"
)

type Client struct {
	Conn       *nats.Conn
	JetStream  jetstream.JetStream
	StreamName string
	consCtxs   map[jetstream.Consumer]jetstream.ConsumeContext
	consMu     sync.Mutex
}

func NewClient(cnf config.NatsConfig) (*Client, error) {
	opts := []nats.Option{
		nats.ReconnectWait(nats.DefaultReconnectWait),
		nats.MaxReconnects(cnf.MaxReconnect),
		nats.Timeout(cnf.ConnectTimeout),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("nats reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if nc.IsReconnecting() {
				log.Printf("nats connection closed: %v", nc.LastError())
			}
		}),
	}

	url := cnf.URL
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	client := &Client{
		Conn:       nc,
		JetStream:  js,
		StreamName: cnf.StreamName,
		consCtxs:   make(map[jetstream.Consumer]jetstream.ConsumeContext),
	}

	if err := client.ensureStream(context.Background()); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	log.Printf("nats jetstream initialized (stream=%s)", cnf.StreamName)
	return client, nil
}

func (c *Client) ensureStream(ctx context.Context) error {
	s, err := c.JetStream.Stream(ctx, c.StreamName)
	if err == nil {
		info, err := s.Info(ctx)
		if err == nil {
			cfg := info.Config
			found := false
			for _, subj := range cfg.Subjects {
				if subj == "data.transfer.>" {
					found = true
					break
				}
			}
			if !found {
				cfg.Subjects = append(cfg.Subjects, "data.transfer.>")
				if _, err := c.JetStream.UpdateStream(ctx, cfg); err != nil {
					return fmt.Errorf("update stream subjects: %w", err)
				}
				log.Printf("nats stream %s subjects updated", c.StreamName)
			}
		}
		return nil
	}
	if err != jetstream.ErrStreamNotFound {
		return fmt.Errorf("check stream: %w", err)
	}

	_, err = c.JetStream.CreateStream(ctx, jetstream.StreamConfig{
		Name: c.StreamName,
		Subjects: []string{
			"operations.>",
			"jobs.>",
			"attempts.>",
			"transfers.>",
			"results.>",
			"control.>",
			"data.transfer.>",
		},
		Retention: jetstream.InterestPolicy,
		MaxAge:    7 * 24 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	log.Printf("nats stream created: %s", c.StreamName)
	return nil
}

func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := c.JetStream.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

func (c *Client) PublishWithHeaders(ctx context.Context, subject string, data []byte, headers map[string]string) error {
	hs := make(nats.Header)
	for k, v := range headers {
		hs.Set(k, v)
	}
	_, err := c.JetStream.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  hs,
		Data:    data,
	})
	if err != nil {
		return fmt.Errorf("publish with headers %s: %w", subject, err)
	}
	return nil
}

func (c *Client) PublishCommand(ctx context.Context, nodeID, commandType string, payload []byte) error {
  subject := NodeCommandSubject(nodeID)
  cmdID := extractField(payload, "command_id")
  opID := extractField(payload, "operation_id")
  jobID := extractField(payload, "job_id")
  transferID := extractNestedField(payload, "payload", "transfer_id")
  log.Printf("NATS_COMMAND_PUBLISH subject=%s command_type=%s command_id=%s operation_id=%s job_id=%s transfer_id=%s destination_node_id=%s",
    subject, commandType, cmdID, opID, jobID, transferID, nodeID)
  if commandType == "START_TRANSFER" {
    fileSize := extractField(payload, "file_size_bytes")
    totalChunks := extractField(payload, "total_chunks")
    fileSHA256 := extractNestedField(payload, "payload", "file_sha256")
    log.Printf("START_TRANSFER_PUBLISH node_id=%s worker_node_id=%s operation_id=%s job_id=%s attempt_id=%s transfer_id=%s file_size_bytes=%s chunk_size_bytes=%s total_chunks=%s file_sha256=%s checksum_algorithm=%s subject=%s",
      nodeID, nodeID, opID, jobID, extractNestedField(payload, "payload", "attempt_id"), transferID, fileSize, extractNestedField(payload, "payload", "chunk_size_bytes"), totalChunks, fileSHA256, extractNestedField(payload, "payload", "checksum_algorithm"), subject)
  }
  _, err := c.JetStream.Publish(ctx, subject, payload)
  if err != nil {
    log.Printf("NATS_COMMAND_PUBLISH_ERROR subject=%s command_type=%s error=%v", subject, commandType, err)
    return fmt.Errorf("publish command %s to %s: %w", commandType, subject, err)
  }
  log.Printf("NATS_COMMAND_PUBLISHED subject=%s command_type=%s command_id=%s", subject, commandType, cmdID)
  return nil
}

func extractField(payload []byte, field string) string {
  var obj map[string]any
  _ = json.Unmarshal(payload, &obj)
  if v, ok := obj[field]; ok {
    switch s := v.(type) {
    case string:
      return s
    case float64:
      return fmt.Sprintf("%v", s)
    }
  }
  return ""
}

func extractNestedField(payload []byte, outer, inner string) string {
  var obj map[string]any
  _ = json.Unmarshal(payload, &obj)
  if outerObj, ok := obj[outer].(map[string]any); ok {
    if v, ok := outerObj[inner]; ok {
      switch s := v.(type) {
      case string:
        return s
      case float64:
        return fmt.Sprintf("%v", s)
      }
    }
  }
  return ""
}

func (c *Client) PublishEvent(ctx context.Context, eventType string, payload []byte) error {
  subject := SubjectCoordinatorEvents
  log.Printf("EVENT_PUBLISH subject=%s event_id=%s event_type=%s operation_id=%s job_id=%s transfer_id=%s",
    subject, extractField(payload, "id"), eventType, extractField(payload, "operation_id"), extractField(payload, "job_id"), extractNestedField(payload, "payload", "transfer_id"))
  _, err := c.JetStream.Publish(ctx, subject, payload)
  if err != nil {
    return fmt.Errorf("publish event %s to %s: %w", eventType, subject, err)
  }
  return nil
}

func (c *Client) Subscribe(ctx context.Context, subject, consumerName string, handler jetstream.MessageHandler) (jetstream.Consumer, error) {
	cons, err := c.JetStream.CreateOrUpdateConsumer(ctx, c.StreamName, jetstream.ConsumerConfig{
		Durable:        consumerName,
		FilterSubjects: []string{subject},
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %s for %s: %w", consumerName, subject, err)
	}

	consCtx, err := cons.Consume(handler)
	if err != nil {
		return nil, fmt.Errorf("consume %s: %w", subject, err)
	}

	c.consMu.Lock()
	c.consCtxs[cons] = consCtx
	c.consMu.Unlock()

	log.Printf("nats consumer started: %s on %s", consumerName, subject)
	return cons, nil
}

func (c *Client) StopConsumer(cons jetstream.Consumer) {
	c.consMu.Lock()
	consCtx, ok := c.consCtxs[cons]
	if ok {
		delete(c.consCtxs, cons)
	}
	c.consMu.Unlock()
	if ok {
		consCtx.Stop()
	}
}

func (c *Client) Close() {
	c.consMu.Lock()
	for cons, consCtx := range c.consCtxs {
		consCtx.Stop()
		delete(c.consCtxs, cons)
	}
	c.consMu.Unlock()

	if c.Conn != nil && c.Conn.IsConnected() {
		if err := c.Conn.Drain(); err != nil {
			log.Printf("nats drain error: %v", err)
		}
		c.Conn.Close()
	}
}
