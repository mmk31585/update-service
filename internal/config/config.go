package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

type ApplicationConfig struct {
	App       AppConfig
	HTTP      HTTPConfig
	DB        DBConfig
	Nats      NatsConfig
	Node      NodeConfig
	File      FileConfig
	Operation OperationConfig
	Elastic   ElasticConfig
}
type AppConfig struct {
	AppName string
	AppEnv  string
	GinMode string
}
type HTTPConfig struct {
	Host            string
	Port            string
	ReadTimeout     time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	CorsOrigin      string
}
type DBConfig struct {
	Host            string
	Port            string
	Name            string
	Username        string
	Password        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	ConnectTimeout  time.Duration
}
type NatsConfig struct {
	URL             string
	ConnectTimeout  time.Duration
	ReconnectPeriod time.Duration
	MaxReconnect    int
	DrainTimeout    time.Duration
	StreamName      string
}
type NodeConfig struct {
	ID                string
	Role              string
	IncarnationID     string
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
}
type FileConfig struct {
	Path            string
	ChunkSize       string
	MaxSize         string
	TransferTimeout time.Duration
	ChunkAckTimeout time.Duration
}
type OperationConfig struct {
	OperationTimeout     time.Duration
	CommandActionTimeout time.Duration
	ProcessingTimeout    time.Duration
}

type ElasticConfig struct {
	Enabled       bool
	URL           string
	Username      string
	Password      string
	Index         string
	Workers       int
	ChannelCap    int
	FlushBytes    int
	FlushInterval time.Duration
	MaxRetries    int
	DLQPath       string
}

func init() {
	_ = godotenv.Load()
}

func LoadConfig() (*ApplicationConfig, error) {
	AppConf := AppConfig{
		AppName: GetString("APP_NAME", "node-service"),
		AppEnv:  GetString("APP_ENV", "development"),
		GinMode: GetString("GIN_MODE", gin.DebugMode),
	}

	HTTPConf := HTTPConfig{
		Host:            GetString("HTTP_HOST", "0.0.0.0"),
		Port:            GetString("HTTP_PORT", "8080"),
		ReadTimeout:     GetDuration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		IdleTimeout:     GetDuration("HTTP_IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout: GetDuration("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second),
		CorsOrigin:      GetString("CORS_ALLOWED_ORIGINS", "http://localhost:3000"),
	}

	DBConf := DBConfig{
		Host:            GetString("DB_HOST", "127.0.0.1"),
		Port:            GetString("DB_PORT", "3306"),
		Name:            GetString("DB_NAME", "file_service"),
		Username:        GetString("DB_USER", "app"),
		Password:        GetString("DB_PASSWORD", "secret"),
		MaxOpenConns:    GetInt("DB_MAX_OPEN_CONNS", 20),
		MaxIdleConns:    GetInt("DB_MAX_IDLE_CONNS", 10),
		ConnMaxLifetime: GetDuration("DB_CONN_MAX_LIFETIME", 30*time.Minute),
		ConnMaxIdleTime: GetDuration("DB_CONN_MAX_IDLE_TIME", 5*time.Minute),
		ConnectTimeout:  GetDuration("DB_CONNECT_TIMEOUT", 5*time.Second),
	}

	NatsConf := NatsConfig{
		URL:             GetString("NATS_URL", "nats://localhost:4222"),
		ConnectTimeout:  GetDuration("NATS_CONNECT_TIMEOUT", 5*time.Second),
		ReconnectPeriod: GetDuration("NATS_RECONNECT_WAIT", 2*time.Second),
		MaxReconnect:    GetInt("NATS_MAX_RECONNECTS", -1),
		DrainTimeout:    GetDuration("NATS_DRAIN_TIMEOUT", 10*time.Second),
		StreamName:      GetString("NATS_STREAM_NAME", "FILE_SERVICE"),
	}

	NodeConf := NodeConfig{
		ID:                GetString("NODE_ID", "node-1"),
		Role:              GetString("NODE_ROLE", "entry"),
		IncarnationID:     GetString("NODE_INCARNATION_ID", generateIncarnationID()),
		HeartbeatInterval: GetDuration("NODE_HEARTBEAT_INTERVAL", 5*time.Second),
		HeartbeatTimeout:  GetDuration("NODE_HEARTBEAT_TIMEOUT", 15*time.Second),
	}

	FileConf := FileConfig{
		Path:            GetString("FILE_STAGING_DIR", "/tmp/transfers"),
		ChunkSize:       GetString("FILE_CHUNK_SIZE", "1MiB"),
		MaxSize:         GetString("FILE_MAX_SIZE", "1GiB"),
		TransferTimeout: GetDuration("FILE_TRANSFER_TIMEOUT", 30*time.Minute),
		ChunkAckTimeout: GetDuration("FILE_CHUNK_ACK_TIMEOUT", 10*time.Second),
	}

	OperationConf := OperationConfig{
		OperationTimeout:     GetDuration("OPERATION_TIMEOUT", 30*time.Minute),
		CommandActionTimeout: GetDuration("COMMAND_ACK_TIMEOUT", 10*time.Second),
		ProcessingTimeout:    GetDuration("PROCESSING_TIMEOUT", 10*time.Minute),
	}

	ElasticConf := ElasticConfig{
		Enabled:       GetBool("ELASTIC_ENABLED", false),
		URL:           GetString("ELASTIC_URL", "http://localhost:9200"),
		Username:      GetString("ELASTIC_USER", "elastic"),
		Password:      GetString("ELASTIC_PASSWORD", "changeme"),
		Index:         GetString("ELASTIC_INDEX_PREFIX", "file-service-operation-completed"),
		Workers:       GetInt("ELASTIC_WORKERS", 2),
		ChannelCap:    GetInt("ELASTIC_CHANNEL_CAP", 1000),
		FlushBytes:    GetInt("ELASTIC_FLUSH_BYTES", 5<<20),
		FlushInterval: GetDuration("ELASTIC_FLUSH_INTERVAL", 5*time.Second),
		MaxRetries:    GetInt("ELASTIC_MAX_RETRIES", 5),
		DLQPath:       GetString("ELASTIC_DLQ_PATH", "/tmp/update/es-dead-letter"),
	}

	return &ApplicationConfig{
		App:       AppConf,
		HTTP:      HTTPConf,
		DB:        DBConf,
		Nats:      NatsConf,
		Node:      NodeConf,
		File:      FileConf,
		Operation: OperationConf,
		Elastic:   ElasticConf,
	}, nil
}

func (c *ApplicationConfig) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("NODE_ID is required")
	}
	if c.Node.Role == "" {
		return fmt.Errorf("NODE_ROLE is required")
	}
	if c.Nats.URL == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if c.DB.Host == "" {
		return fmt.Errorf("DB_HOST is required")
	}
	if c.DB.Name == "" {
		return fmt.Errorf("DB_NAME is required")
	}
	if c.DB.Username == "" {
		return fmt.Errorf("DB_USER is required")
	}
	return nil
}
func GetString(key string, fallback string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return val
}
func GetDuration(key string, fallback time.Duration) time.Duration {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	parsed, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return parsed
}
func GetInt(key string, fallback int) int {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	valAsInt, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return valAsInt
}

func GetBool(key string, fallback bool) bool {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}
	return b
}

func ParseSize(s string) int64 {
	switch {
	case s == "":
		return 1 << 20
	case strings.HasSuffix(s, "KiB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "KiB"), 10, 64)
		return n * 1024
	case strings.HasSuffix(s, "MiB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "MiB"), 10, 64)
		return n * 1024 * 1024
	case strings.HasSuffix(s, "GiB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "GiB"), 10, 64)
		return n * 1024 * 1024 * 1024
	default:
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
}

func generateIncarnationID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return "inc-" + hex.EncodeToString(buf)
}
