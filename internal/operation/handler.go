package operation

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"update/internal/application/operations"
	"update/internal/domain/operation"
)

type CreateOperationRequest struct {
	Processor       string          `json:"processor" validate:"required"`
	Options         map[string]any  `json:"options,omitempty"`
	Metadata        map[string]any  `json:"metadata,omitempty"`
	Deadline        *time.Time      `json:"deadline,omitempty"`
	RetryPolicy     *RetryPolicyReq `json:"retry_policy,omitempty"`
	ClientReference string          `json:"client_reference,omitempty"`
}

type RetryPolicyReq struct {
	MaxAttempts int `json:"max_attempts"`
}

var validate = validator.New(validator.WithRequiredStructEnabled())

type Handler struct {
	app      HealthChecker
	createUC *operations.CreateOperationUseCase
	listUC   *operations.ListOperationUseCase
	fileUC   *operations.FileOperationUseCase
	cancelUC *operations.CancelOperationUseCase
	retryUC  *operations.RetryOperationUseCase
	resultUC *operations.GetResultOperationUseCase
	getUC    *operations.GetOperationUseCase
	baseURL  string
}

type HealthChecker interface {
	ReadinessCheck() error
}

func NewHandler(
	app HealthChecker,
	createUC *operations.CreateOperationUseCase,
	listUC *operations.ListOperationUseCase,
	fileUC *operations.FileOperationUseCase,
	cancelUC *operations.CancelOperationUseCase,
	retryUC *operations.RetryOperationUseCase,
	resultUC *operations.GetResultOperationUseCase,
	getUC *operations.GetOperationUseCase,
	baseURL string,
) *Handler {
	return &Handler{
		app:      app,
		createUC: createUC,
		listUC:   listUC,
		fileUC:   fileUC,
		cancelUC: cancelUC,
		retryUC:  retryUC,
		resultUC: resultUC,
		getUC:    getUC,
		baseURL:  baseURL,
	}
}

// CreateOperations godoc
// @Summary Create an operation
// @Description Creates the durable operation row and initial append-only event. Does not schedule work before the input upload is complete.
// @Tags operations
// @Accept json
// @Produce json
// @Param   Idempotency-Key header    string  true   "Idempotency key" example(ops-create-001)
// @Param   createOperationRequest body    CreateOperationRequest  true  "Create operation request" example({"processor":"image-processor","options":{"quality":90},"metadata":{"source":"web"},"client_reference":"ref-123"})
// @Success 202 {object} operation.CreateOperationResponse
// @Failure 400 {object} operation.ErrorResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 409 {object} operation.ErrorResponse
// @Failure 422 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations [post]
func (h *Handler) CreateOperations(c *gin.Context) {
	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}

	var req CreateOperationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	if err := validate.Struct(req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "validation failed", "details": err.Error()})
		return
	}

	retryPolicy := (*operation.RetryPolicy)(nil)
	if req.RetryPolicy != nil {
		retryPolicy = &operation.RetryPolicy{MaxAttempts: req.RetryPolicy.MaxAttempts}
	}

	input := operations.CreateOperationInput{
		Processor:       req.Processor,
		Options:         req.Options,
		Metadata:        req.Metadata,
		Deadline:        req.Deadline,
		RetryPolicy:     retryPolicy,
		ClientReference: req.ClientReference,
	}

	out, err := h.createUC.Execute(c.Request.Context(), input, idempotencyKey, c.Request.Method, c.Request.URL.Path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"operation_id":  out.OperationID.String(),
		"status":        out.Status,
		"stage":         out.Stage,
		"upload_target": out.UploadTarget,
		"status_url":    out.StatusURL,
		"result_url":    out.ResultURL,
		"created_at":    out.CreatedAt.Format(time.RFC3339),
		"version":       out.Version,
	})
}

// ListOperations godoc
// @Summary List operations
// @Description Returns a paginated list of operations. Status reads are authoritative MariaDB reads.
// @Tags operations
// @Accept json
// @Produce json
// @Param   status          query     string  false  "Filter by operation status (e.g. QUEUED, COMPLETED)" example(QUEUED)
// @Param   processor       query     string  false  "Filter by processor name" example(image-processor)
// @Param   cursor          query     string  false  "Pagination cursor" example(cursor-abc)
// @Param   limit           query     int     false  "Page size (default 50)" example(20)
// @Success 200 {object} operation.ListOperationsResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations [get]
func (h *Handler) ListOperations(c *gin.Context) {
	status := c.Query("status")
	processor := c.Query("processor")
	cursor := c.Query("cursor")
	limit := 50

	var statusPtr *operation.OperationStatus
	if status != "" {
		s := operation.OperationStatus(status)
		statusPtr = &s
	}
	var processorPtr *string
	if processor != "" {
		processorPtr = &processor
	}
	var cursorPtr *string
	if cursor != "" {
		cursorPtr = &cursor
	}

	input := operations.ListOperationInput{
		Status:    statusPtr,
		Processor: processorPtr,
		Cursor:    cursorPtr,
		Limit:     limit,
	}

	out, err := h.listUC.Execute(c.Request.Context(), input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	items := make([]gin.H, len(out.Operations))
	for i, op := range out.Operations {
		items[i] = gin.H{
			"operation_id": op.ID.String(),
			"status":       op.Status,
			"stage":        op.Stage,
			"processor":    op.Processor,
			"retry_count":  op.RetryCount,
			"max_attempts": op.MaxAttempts,
			"created_at":   op.CreatedAt.Format(time.RFC3339),
			"updated_at":   op.UpdatedAt.Format(time.RFC3339),
			"version":      op.Version,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"items":       items,
		"next_cursor": out.NextCursor,
	})
}

// GetOperation godoc
// @Summary Read operation status
// @Description Status reads are authoritative MariaDB reads served by an entry-capable instance.
// @Tags operations
// @Accept json
// @Produce json
// @Param   If-None-Match   header    string  false  "ETag for conditional request" example("1")
// @Param   id              path      string  true   "Operation ID" example(op-abc123)
// @Success 200 {object} operation.OperationResponse
// @Failure 304 {object} operation.OperationResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 404 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations/{id} [get]
func (h *Handler) GetOperation(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "operation id is required"})
		return
	}

	op, err := h.getUC.Execute(c.Request.Context(), operation.OperationID(id))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if op == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
		return
	}

	resp := gin.H{
		"operation_id": op.ID.String(),
		"status":       op.Status,
		"stage":        op.Stage,
		"processor":    op.Processor,
		"retry_count":  op.RetryCount,
		"max_attempts": op.MaxAttempts,
		"created_at":   op.CreatedAt.Format(time.RFC3339),
		"updated_at":   op.UpdatedAt.Format(time.RFC3339),
		"version":      op.Version,
		"status_url":   h.baseURL + "/operations/" + op.ID.String(),
		"result_url":   h.baseURL + "/operations/" + op.ID.String() + "/result",
	}

	if op.Deadline != nil {
		resp["deadline"] = op.Deadline.Format(time.RFC3339)
	}
	if op.InputFile != nil {
		resp["input_file"] = gin.H{
			"size_bytes": op.InputFile.SizeBytes,
			"sha256":     op.InputFile.SHA256,
			"file_name":  op.InputFile.FileName,
		}
	}
	if op.CurrentJobID != nil {
		resp["current_job_id"] = op.CurrentJobID.String()
	}
	if op.CurrentAttemptID != nil {
		resp["current_attempt_id"] = op.CurrentAttemptID.String()
	}
	if op.CurrentTransferID != nil {
		resp["current_transfer_id"] = op.CurrentTransferID.String()
	}
	if op.CancelRequestedAt != nil {
		resp["cancel_requested_at"] = op.CancelRequestedAt.Format(time.RFC3339)
	}
	if op.Error != nil {
		resp["error"] = gin.H{
			"code":    op.Error.Code,
			"message": op.Error.Message,
		}
	}

	c.JSON(http.StatusOK, resp)
}

// FileOperations godoc
// @Summary Stream the input file
// @Description The client streams bytes to the entry upload endpoint. The entry streams them to a node-local ingress file, enforces size limits, computes SHA-256 incrementally, and records the file metadata. Supports raw binary body (application/octet-stream) or multipart form data with a "file" field.
// @Tags operations
// @Accept multipart/form-data
// @Accept application/octet-stream
// @Produce json
// @Param   Idempotency-Key header    string  true   "Idempotency key" example(ops-file-001)
// @Param   If-Match         header    string  false  "Operation version precondition" example("1")
// @Param   Content-Length   header    integer false  "Optional, must match bytes received" example(1048576)
// @Param   Digest           header    string  false  "SHA-256 digest of request body" example(sha256=abc123...)
// @Param   X-File-Size      header    integer false  "Declared byte count" example(1048576)
// @Param   X-File-Name      header    string  false  "Client filename metadata" example(image.png)
// @Param   id               path      string  true   "Operation ID" example(op-abc123)
// @Param   file             formData  file    true   "File to upload"
// @Success 202 {object} operation.FileOperationResponse
// @Failure 200 {object} operation.FileOperationResponse
// @Failure 400 {object} operation.ErrorResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 404 {object} operation.ErrorResponse
// @Failure 409 {object} operation.ErrorResponse
// @Failure 412 {object} operation.ErrorResponse
// @Failure 413 {object} operation.ErrorResponse
// @Failure 422 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations/{id}/file [put]
func (h *Handler) FileOperations(c *gin.Context) {
	log.Printf("handler: FileOperations called for %s", c.Param("id"))
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "operation id is required"})
		return
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}

	// Extract headers for digest, size, filename
	headers := make(map[string]string)
	if digest := c.GetHeader("Digest"); digest != "" {
		headers["Digest"] = digest
	}
	if size := c.GetHeader("X-File-Size"); size != "" {
		headers["X-File-Size"] = size
	}
	if name := c.GetHeader("X-File-Name"); name != "" {
		headers["X-File-Name"] = name
	}

	var body io.Reader
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := c.Request.ParseMultipartForm(32 << 20); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid multipart form"})
			return
		}
		file, _, err := c.Request.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file field is required"})
			return
		}
		defer file.Close()
		body = file
	} else {
		body = c.Request.Body
	}

	input := operations.FileOperationInput{
		OperationID: operation.OperationID(id),
		Body:        body,
		Headers:     headers,
	}

	out, err := h.fileUC.Execute(c.Request.Context(), input, idempotencyKey, c.Request.Method, c.Request.URL.Path)
	if err != nil {
		if errors.Is(err, operations.ErrOperationNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
		} else if errors.Is(err, operations.ErrInvalidState) {
			c.JSON(http.StatusConflict, gin.H{"error": "operation not in CREATED state"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"operation_id": out.OperationID.String(),
		"status":       out.Status,
		"stage":        out.Stage,
		"input_file": gin.H{
			"size_bytes": out.InputFile.SizeBytes,
			"sha256":     out.InputFile.SHA256,
			"file_name":  out.InputFile.FileName,
		},
		"status_url": out.StatusURL,
		"result_url": out.ResultURL,
		"updated_at": out.UpdatedAt.Format(time.RFC3339),
		"version":    out.Version,
	})
}

// CancelOperations godoc
// @Summary Cancel an operation
// @Description Cancellation is an idempotent intent. Persists cancel_requested_at and emits an event.
// @Tags operations
// @Accept json
// @Produce json
// @Param   Idempotency-Key header    string  true   "Idempotency key" example(ops-cancel-001)
// @Param   If-Match         header    string  false  "Operation version precondition" example("2")
// @Param   id               path      string  true   "Operation ID" example(op-abc123)
// @Param   cancelRequest    body      operation.CancelOperationInput  false  "Cancellation reason" example({"reason":"user request"})
// @Success 202 {object} operation.CancelOperationResponse
// @Failure 200 {object} operation.CancelOperationResponse
// @Failure 400 {object} operation.ErrorResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 404 {object} operation.ErrorResponse
// @Failure 409 {object} operation.ErrorResponse
// @Failure 412 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations/{id}/cancel [post]
func (h *Handler) CancelOperations(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "operation id is required"})
		return
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}

	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	input := operations.CancelOperationInput{
		OperationID: operation.OperationID(id),
		Reason:      req.Reason,
	}

	out, err := h.cancelUC.Execute(c.Request.Context(), input)
	if err != nil {
		if errors.Is(err, operations.ErrOperationNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
			return
		}
		if errors.Is(err, operations.ErrAlreadyTerminal) {
			c.JSON(http.StatusConflict, gin.H{"error": "operation is in terminal state, cancellation not applicable"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"operation_id":        out.OperationID.String(),
		"status":              out.Status,
		"cancel_requested_at": out.CancelRequestedAt.Format(time.RFC3339),
		"status_url":          out.StatusURL,
		"version":             out.Version,
	})
}

// RetryOperations godoc
// @Summary Retry an operation
// @Description Retry is allowed only from FAILED or TIMEOUT, subject to budget and policy.
// @Tags operations
// @Accept json
// @Produce json
// @Param   Idempotency-Key header    string  true   "Idempotency key" example(ops-retry-001)
// @Param   If-Match         header    string  false  "Operation version precondition" example("2")
// @Param   id               path      string  true   "Operation ID" example(op-abc123)
// @Param   retryRequest     body      operation.RetryOperationInput  false  "Retry reason" example({"reason":"timeout exceeded"})
// @Success 202 {object} operation.RetryOperationResponse
// @Failure 200 {object} operation.RetryOperationResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 404 {object} operation.ErrorResponse
// @Failure 409 {object} operation.ErrorResponse
// @Failure 412 {object} operation.ErrorResponse
// @Failure 422 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations/{id}/retry [post]
func (h *Handler) RetryOperations(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "operation id is required"})
		return
	}

	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}

	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
		return
	}

	input := operations.RetryOperationInput{
		OperationID: operation.OperationID(id),
		Reason:      req.Reason,
	}

	out, err := h.retryUC.Execute(c.Request.Context(), input)
	if err != nil {
		if errors.Is(err, operations.ErrOperationNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
			return
		}
		if errors.Is(err, operations.ErrInvalidStateForRetry) {
			c.JSON(http.StatusConflict, gin.H{"error": "operation not in FAILED or TIMEOUT state, or retry not allowed"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"operation_id":       out.OperationID.String(),
		"status":             out.Status,
		"retry_count":        out.RetryCount,
		"max_attempts":       out.MaxAttempts,
		"current_job_id":     out.CurrentJobID.String(),
		"current_attempt_id": out.CurrentAttemptID.String(),
		"status_url":         out.StatusURL,
		"updated_at":         out.UpdatedAt.Format(time.RFC3339),
		"version":            out.Version,
	})
}

// ResultOperations godoc
// @Summary Retrieve a result
// @Description Returns the single structured result for a completed operation.
// @Tags operations
// @Accept json
// @Produce json
// @Param   id               path      string  true   "Operation ID"
// @Success 200 {object} operation.ResultResponse
// @Failure 401 {object} operation.ErrorResponse
// @Failure 403 {object} operation.ErrorResponse
// @Failure 404 {object} operation.ErrorResponse
// @Failure 409 {object} operation.ErrorResponse
// @Failure 410 {object} operation.ErrorResponse
// @Failure 503 {object} operation.ErrorResponse
// @Router /operations/{id}/result [get]
func (h *Handler) ResultOperations(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "operation id is required"})
		return
	}

	input := operations.GetResultOperationInput{
		OperationID: operation.OperationID(id),
	}

	out, err := h.resultUC.Execute(c.Request.Context(), input)
	if err != nil {
		if errors.Is(err, operations.ErrOperationNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
			return
		}
		if errors.Is(err, operations.ErrResultNotReady) {
			c.JSON(http.StatusConflict, gin.H{"error": "result not ready - operation not COMPLETED"})
			return
		}
		if errors.Is(err, operations.ErrResultExpired) {
			c.JSON(http.StatusGone, gin.H{"error": "result expired - removed by retention policy"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	res := out.Result
	c.JSON(http.StatusOK, gin.H{
		"operation_id":   res.OperationID,
		"result_id":      res.ID.String(),
		"processor":      res.Processor,
		"schema_version": res.SchemaVersion,
		"value":          res.Value,
		"produced_at":    res.ProducedAt.Format(time.RFC3339),
	})
}

// Health godoc
// @Summary Health check
// @Description Returns the service health status
// @Tags health
// @Produce json
// @Success 200 {object} gin.H
// @Router /health [get]
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// HealthLive godoc
// @Summary Liveness check
// @Description Returns the service liveness status
// @Tags health
// @Produce json
// @Success 200 {object} gin.H
// @Router /health/live [get]
func (h *Handler) HealthLive(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "alive"})
}

// HealthReady godoc
// @Summary Readiness check
// @Description Returns the service readiness status including dependency checks
// @Tags health
// @Produce json
// @Success 200 {object} gin.H
// @Failure 503 {object} gin.H
// @Router /health/ready [get]
func (h *Handler) HealthReady(c *gin.Context) {
	if err := h.app.ReadinessCheck(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
			"error":  err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
