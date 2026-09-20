package app

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"

	"update/internal/config"
	mariadb "update/internal/infrastructure/mariadb/repositories"
	"update/internal/infrastructure/nats"
	"update/internal/infrastructure/transfer"
	"update/internal/infrastructure/elasticsearch"
)

func Bootstrap(ctx context.Context) (*Application, error) {
	cnf, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}

	gin.SetMode(cnf.App.GinMode)

	if err := cnf.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	dsn := buildDSN(cnf.DB)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	mdb := mariadb.NewDB(db)
	mdb.SetMaxOpenConns(cnf.DB.MaxOpenConns)
	mdb.SetMaxIdleConns(cnf.DB.MaxIdleConns)
	mdb.SetConnMaxLifetime(cnf.DB.ConnMaxLifetime)
	mdb.SetConnMaxIdleTime(cnf.DB.ConnMaxIdleTime)

	if err := goose.SetDialect("mysql"); err != nil {
		return nil, fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.Up(mdb.DB, "internal/infrastructure/mariadb/migrations"); err != nil {
		return nil, fmt.Errorf("migrations: %w", err)
	}
	log.Println("database migrations applied")
	opRepo := mariadb.NewOperationRepository(mdb)
	jobRepo := mariadb.NewJobRepository(mdb)
	attemptRepo := mariadb.NewJobAttemptRepository(mdb)
	transferRepo := mariadb.NewFileTransferRepository(mdb)
	chunkRepo := mariadb.NewFileChunkRepository(mdb)
	resultRepo := mariadb.NewResultRepository(mdb)
	idempotencyRepo := mariadb.NewIdempotencyRepository(mdb)
	eventRepo := mariadb.NewOperationEventRepository(mdb)
	nodeRepo := mariadb.NewNodeRepository(mdb)
	heartbeatRepo := mariadb.NewHeartbeatRepository(mdb)

	nc, err := nats.NewClient(cnf.Nats)
	if err != nil {
		return nil, fmt.Errorf("init nats: %w", err)
	}

	stagingDir := cnf.File.Path
	if stagingDir == "" {
		stagingDir = "/tmp/file-service-staging"
	}

	transferReceiver := transfer.NewTransferReceiver(opRepo, transferRepo, chunkRepo, eventRepo, nc, stagingDir, cnf.Node.ID)

	var elasticClient *elasticsearch.Client
	var elasticIndexer *elasticsearch.Indexer
	if cnf.Elastic.Enabled {
		elasticClient, err = elasticsearch.NewClient(cnf.Elastic.URL, cnf.Elastic.Username, cnf.Elastic.Password, cnf.Elastic.Index)
		if err != nil {
			log.Printf("warning: elasticsearch init failed: %v", err)
		} else {
			elasticIndexer = elasticsearch.NewIndexer(elasticClient, cnf.Elastic.Index, cnf.Node.ID)
		}
	}

	return New(cnf, opRepo, jobRepo, attemptRepo, transferRepo, chunkRepo, resultRepo, idempotencyRepo, eventRepo, nodeRepo, heartbeatRepo, transferReceiver, nc, db, elasticClient, elasticIndexer), nil
}

func buildDSN(dbConf config.DBConfig) string {
	host := dbConf.Host
	if host == "localhost" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true",
		dbConf.Username,
		dbConf.Password,
		host,
		dbConf.Port,
		dbConf.Name,
	)
}
