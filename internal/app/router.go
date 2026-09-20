package app

import (
	"strings"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	"update/internal/application/operations"
	"update/internal/application/scheduling"
	"update/internal/config"
	"update/internal/operation"

	"github.com/gin-gonic/gin"
)

func (app *Application) RegisterRouter(r *gin.Engine) {
	app.router = r

	baseURL := "http://" + app.AppConfig.HTTP.Host + ":" + app.AppConfig.HTTP.Port
	if app.AppConfig.HTTP.CorsOrigin != "" {
		baseURL = strings.TrimSuffix(app.AppConfig.HTTP.CorsOrigin, "/")
	}

	stagingDir := app.AppConfig.File.Path
	if stagingDir == "" {
		stagingDir = "/tmp/file-service-staging"
	}
	chunkSize := config.ParseSize(app.AppConfig.File.ChunkSize)
	maxFileSize := config.ParseSize(app.AppConfig.File.MaxSize)

	createUC := operations.NewCreateOperationUseCase(app.operations, app.idempotency, app.events, baseURL)
	listUC := operations.NewListOperationUseCase(app.operations)
	dispatchUC := operations.NewDispatchOperationUseCase(app.operations, app.jobs, app.attempts, app.events, app.nats, scheduling.NewScheduler(app.nodes), baseURL, app.transfers, app.chunks, stagingDir)
	fileUC := operations.NewFileOperationUseCase(
		app.operations,
		app.events,
		stagingDir,
		chunkSize,
		maxFileSize,
		baseURL,
		dispatchUC,
	)
	cancelUC := operations.NewCancelOperationUseCase(app.operations, app.events, baseURL)
	retryUC := operations.NewRetryOperationUseCase(app.operations, app.jobs, app.attempts, app.events, baseURL)
	resultUC := operations.NewGetResultOperationUseCase(app.operations, app.results)
	getUC := operations.NewGetOperationUseCase(app.operations, baseURL)

	handler := operation.NewHandler(app, createUC, listUC, fileUC, cancelUC, retryUC, resultUC, getUC, baseURL)

	// Global middleware
	r.Use(CorrelationMiddleware())
	r.Use(RecoveryMiddleware())
	r.Use(SanitizeErrorMiddleware())
	r.Use(CORSMiddleware(strings.Split(app.AppConfig.HTTP.CorsOrigin, ",")))

	api := r.Group("/api/v1")
	{
		api.GET("health", handler.Health)
		api.GET("health/live", handler.HealthLive)
		api.GET("health/ready", handler.HealthReady)

		operations := api.Group("/operations")
		{
			operations.POST("", AuthMiddleware(), ContentTypeMiddleware(), handler.CreateOperations)
			operations.GET("", AuthMiddleware(), handler.ListOperations)
			operations.GET("/:id", AuthMiddleware(), handler.GetOperation)

			operations.PUT("/:id/file", AuthMiddleware(), ContentTypeMiddleware(), RequestSizeLimitMiddleware(1<<30), handler.FileOperations)
			operations.POST("/:id/cancel", AuthMiddleware(), ContentTypeMiddleware(), handler.CancelOperations)
			operations.POST("/:id/retry", AuthMiddleware(), ContentTypeMiddleware(), handler.RetryOperations)
			operations.GET("/:id/result", AuthMiddleware(), handler.ResultOperations)
		}
	}

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
}
