package elasticsearch

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"update/internal/domain/operation"
	"update/internal/domain/event"
)

const (
	defaultChannelCap = 1000
	defaultWorkers    = 2
	defaultFlushBytes = 5 << 20
	defaultFlushInt   = 5 * time.Second
	defaultMaxRetries = 5
	dlqDir            = "/tmp/update/es-dead-letter"
)

type Indexer struct {
	client       *Client
	index        string
	nodeID       string
	ch           chan indexedDoc
	wg           sync.WaitGroup
	mu           sync.Mutex
	buffer       []indexedDoc
	bufBytes     int
	bufCount     int
	flushCh      chan struct{}
	closed       chan struct{}
}

type indexedDoc struct {
	doc  []byte
	id   string
	opID string
}

func NewIndexer(client *Client, index, nodeID string) *Indexer {
	return &Indexer{
		client:  client,
		index:   index,
		nodeID:  nodeID,
		ch:      make(chan indexedDoc, defaultChannelCap),
		buffer:  make([]indexedDoc, 0, 100),
		flushCh: make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
}

func (i *Indexer) Start(ctx context.Context) {
	for w := 0; w < defaultWorkers; w++ {
		i.wg.Add(1)
		go i.worker(ctx, w)
	}
	go i.flusher(ctx)
	log.Printf("elastic indexer started with %d workers, channel cap=%d", defaultWorkers, defaultChannelCap)
}

func (i *Indexer) Index(ctx context.Context, op *operation.Operation, evt *event.OperationEvent) {
	select {
	case i.ch <- indexedDoc{
		doc:  i.buildDoc(op, evt),
		id:   op.ID.String(),
		opID: op.ID.String(),
	}:
	default:
		log.Printf("elastic indexer channel full, sending to DLQ for op=%s", op.ID.String())
		i.dlq(op, evt)
	}
}

func (i *Indexer) Stop(ctx context.Context) {
	close(i.closed)
	i.wg.Wait()
	log.Println("elastic indexer stopped")
}

func (i *Indexer) worker(ctx context.Context, id int) {
	defer i.wg.Done()
	log.Printf("elastic worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			i.drain(ctx)
			return
		case <-i.closed:
			i.drain(ctx)
			return
		case doc := <-i.ch:
			if err := i.indexDoc(ctx, doc); err != nil {
				log.Printf("elastic worker %d: failed to index op=%s: %v", id, doc.opID, err)
				i.dlqFromDoc(doc)
			}
		}
	}
}

func (i *Indexer) indexDoc(ctx context.Context, doc indexedDoc) error {
	return i.client.Index(ctx, doc.doc, doc.id)
}

func (i *Indexer) flusher(ctx context.Context) {
	ticker := time.NewTicker(defaultFlushInt)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			i.flush()
			return
		case <-i.closed:
			i.flush()
			return
		case <-ticker.C:
			i.flush()
		case <-i.flushCh:
			i.flush()
		}
	}
}

func (i *Indexer) flush() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.buffer) == 0 {
		return
	}
	log.Printf("elastic flusher: flushing %d docs (%d bytes)", len(i.buffer), i.bufBytes)
	i.buffer = i.buffer[:0]
	i.bufBytes = 0
	i.bufCount = 0
}

func (i *Indexer) drain(ctx context.Context) {
	for len(i.ch) > 0 {
		select {
		case doc := <-i.ch:
			if err := i.indexDoc(ctx, doc); err != nil {
				log.Printf("elastic drain: failed to index op=%s: %v", doc.opID, err)
				i.dlqFromDoc(doc)
			}
		default:
			return
		}
	}
}

func (i *Indexer) buildDoc(op *operation.Operation, evt *event.OperationEvent) []byte {
	doc := BuildCompletedDocument(op, evt, i.nodeID)
	b, _ := json.Marshal(doc)
	return b
}

func (i *Indexer) dlq(op *operation.Operation, evt *event.OperationEvent) {
	i.dlqFromDoc(indexedDoc{
		doc:  i.buildDoc(op, evt),
		id:   op.ID.String(),
		opID: op.ID.String(),
	})
}

func (i *Indexer) dlqFromDoc(doc indexedDoc) {
	entry := struct {
		FailedAt  time.Time `json:"failed_at"`
		OpID      string    `json:"operation_id"`
		Document  []byte    `json:"document"`
	}{
		FailedAt: time.Now().UTC(),
		OpID:     doc.opID,
		Document: doc.doc,
	}
	b, _ := json.Marshal(entry)

	_ = os.MkdirAll(dlqDir, 0755)
	logPath := filepath.Join(dlqDir, "dead-letter.log")
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("elastic DLQ: cannot open file: %v", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("elastic DLQ: cannot write: %v", err)
	}
	log.Printf("elastic DLQ: saved failed doc op=%s", doc.opID)
}
