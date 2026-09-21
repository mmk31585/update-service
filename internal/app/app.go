package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"update/internal/application/ports"
	"update/internal/config"
	"update/internal/infrastructure/elasticsearch"
	"update/internal/infrastructure/nats"
	"update/internal/infrastructure/transfer"

	"github.com/gin-gonic/gin"
)

type Application struct {
	AppConfig *config.ApplicationConfig
	router    *gin.Engine
	db        *sql.DB

	elasticClient  *elasticsearch.Client
	elasticIndexer *elasticsearch.Indexer

	operations       ports.OperationRepository
	jobs             ports.JobRepository
	attempts         ports.JobAttemptRepository
	transfers        ports.FileTransferRepository
	chunks           ports.FileChunkRepository
	results          ports.ResultRepository
	idempotency      ports.IdempotencyRepository
	events           ports.OperationEventRepository
	nodes            ports.NodeRepository
	heartbeats       ports.HeartbeatRepository
	transferReceiver *transfer.TransferReceiver
	nats             *nats.Client
}

func New(cnf *config.ApplicationConfig, ops ports.OperationRepository, jobs ports.JobRepository,
	attempts ports.JobAttemptRepository, transfers ports.FileTransferRepository,
	chunks ports.FileChunkRepository, results ports.ResultRepository,
	idempotency ports.IdempotencyRepository, events ports.OperationEventRepository,
	nodes ports.NodeRepository, heartbeats ports.HeartbeatRepository,
	transferReceiver *transfer.TransferReceiver,
	natsClient *nats.Client, db *sql.DB, elasticClient *elasticsearch.Client, elasticIndexer *elasticsearch.Indexer) *Application {
	return &Application{
		AppConfig:        cnf,
		db:               db,
		elasticClient:    elasticClient,
		elasticIndexer:   elasticIndexer,
		operations:       ops,
		jobs:             jobs,
		attempts:         attempts,
		transfers:        transfers,
		chunks:           chunks,
		results:          results,
		idempotency:      idempotency,
		events:           events,
		nodes:            nodes,
		heartbeats:       heartbeats,
		transferReceiver: transferReceiver,
		nats:             natsClient,
	}
}

func (app *Application) Run(ctx context.Context) error {
	if app.router == nil {
		return fmt.Errorf("router not initialized: call RegisterRouter before Run")
	}

	addr := app.AppConfig.HTTP.Host + ":" + app.AppConfig.HTTP.Port

	srv := &http.Server{
		Addr:              addr,
		Handler:           app.router,
		ReadTimeout:       app.AppConfig.HTTP.ReadTimeout,
		ReadHeaderTimeout: app.AppConfig.HTTP.ReadTimeout,
		IdleTimeout:       app.AppConfig.HTTP.IdleTimeout,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	roleMgr := NewRoleManager(app)
	if err := roleMgr.Start(ctx); err != nil {
		return fmt.Errorf("start role tasks: %w", err)
	}

	if app.elasticIndexer != nil {
		app.elasticIndexer.Start(ctx)
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-signalCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), app.AppConfig.HTTP.ShutdownTimeout)
	defer cancel()

	log.Println("shutting down server...")
	if app.elasticIndexer != nil {
		app.elasticIndexer.Stop(shutdownCtx)
	}
	if app.nats != nil {
		app.nats.Close()
	}
	if app.db != nil {
		if err := app.db.Close(); err != nil {
			log.Printf("close database: %v", err)
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown failed: %w", err)
	}

	log.Println("server stopped")
	return nil
}

func (app *Application) HealthCheck() map[string]string {
	return map[string]string{
		"status":  "ok",
		"node_id": app.AppConfig.Node.ID,
		"role":    string(app.AppConfig.Node.Role),
	}
}

func (app *Application) ReadinessCheck() error {
	if app.nats == nil || !app.nats.Conn.IsConnected() {
		return fmt.Errorf("nats not connected")
	}
	return nil
}
