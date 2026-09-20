package main

// @title           File Processing Service API
// @version         1.0
// @description     HTTP API for file upload, processing orchestration, and result retrieval
// @host            localhost:8080
// @BasePath        /api/v1
// @schemes         http https
//
// @securityDefinitions.apikey Authorization
// @in header
// @name Authorization
// @description Type "Bearer <token>" or "Basic <credentials>"
//
// @securityDefinitions.apikey Idempotency-Key
// @in header
// @name Idempotency-Key
// @description Required for create, upload, cancel, and retry requests
import (
	"context"
	"log"

	_ "update/docs"

	"update/internal/app"

	"github.com/gin-gonic/gin"
)

func main() {
	ctx := context.Background()
	application, err := app.Bootstrap(ctx)
	if err != nil {
		log.Fatal(err)
	}

	router := gin.Default()
	application.RegisterRouter(router)

	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
