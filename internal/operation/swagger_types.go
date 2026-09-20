package operation

import "time"

// CreateOperationResponse represents the response for creating an operation
type CreateOperationResponse struct {
	OperationID  string       `json:"operation_id"`
	Status       string       `json:"status"`
	Stage        string       `json:"stage"`
	UploadTarget UploadTarget `json:"upload_target"`
	StatusURL    string       `json:"status_url"`
	ResultURL    string       `json:"result_url"`
	CreatedAt    time.Time    `json:"created_at"`
	Version      int          `json:"version"`
}

// UploadTarget represents the upload target in the create operation response
type UploadTarget struct {
	Method   string            `json:"method"`
	URL      string            `json:"url"`
	Headers  map[string]string `json:"headers"`
	Expires  time.Time         `json:"expires_at"`
	MaxBytes int64             `json:"max_bytes"`
}

// ListOperationsResponse represents the response for listing operations
type ListOperationsResponse struct {
	Items      []OperationItem `json:"items"`
	NextCursor *string         `json:"next_cursor"`
}

// OperationItem represents a simplified operation in list responses
type OperationItem struct {
	OperationID string    `json:"operation_id"`
	Status      string    `json:"status"`
	Stage       string    `json:"stage"`
	Processor   string    `json:"processor"`
	RetryCount  int       `json:"retry_count"`
	MaxAttempts int       `json:"max_attempts"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Version     int       `json:"version"`
}

// OperationResponse represents the full operation detail response
type OperationResponse struct {
	OperationID       string          `json:"operation_id"`
	Status            string          `json:"status"`
	Stage             string          `json:"stage"`
	Processor         string          `json:"processor"`
	RetryCount        int             `json:"retry_count"`
	MaxAttempts       int             `json:"max_attempts"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	Deadline          *time.Time      `json:"deadline,omitempty"`
	InputFile         *InputFile      `json:"input_file,omitempty"`
	CurrentJobID      *string         `json:"current_job_id,omitempty"`
	CurrentAttemptID  *string         `json:"current_attempt_id,omitempty"`
	CurrentTransferID *string         `json:"current_transfer_id,omitempty"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`
	Error             *OperationError `json:"error,omitempty"`
	StatusURL         string          `json:"status_url"`
	ResultURL         string          `json:"result_url"`
	Version           int             `json:"version"`
}

// InputFile represents the input file metadata
type InputFile struct {
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	FileName  string `json:"file_name"`
}

// OperationError represents an operation error
type OperationError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// FileOperationResponse represents the response for file upload
type FileOperationResponse struct {
	OperationID string    `json:"operation_id"`
	Status      string    `json:"status"`
	Stage       string    `json:"stage"`
	InputFile   InputFile `json:"input_file"`
	StatusURL   string    `json:"status_url"`
	ResultURL   string    `json:"result_url"`
	UpdatedAt   time.Time `json:"updated_at"`
	Version     int       `json:"version"`
}

// CancelOperationResponse represents the response for cancelling an operation
type CancelOperationResponse struct {
	OperationID       string    `json:"operation_id"`
	Status            string    `json:"status"`
	CancelRequestedAt time.Time `json:"cancel_requested_at"`
	StatusURL         string    `json:"status_url"`
	Version           int       `json:"version"`
}

// RetryOperationResponse represents the response for retrying an operation
type RetryOperationResponse struct {
	OperationID      string    `json:"operation_id"`
	Status           string    `json:"status"`
	RetryCount       int       `json:"retry_count"`
	MaxAttempts      int       `json:"max_attempts"`
	CurrentJobID     string    `json:"current_job_id"`
	CurrentAttemptID string    `json:"current_attempt_id"`
	StatusURL        string    `json:"status_url"`
	UpdatedAt        time.Time `json:"updated_at"`
	Version          int       `json:"version"`
}

// ResultResponse represents the response for retrieving a result
type ResultResponse struct {
	OperationID   string                 `json:"operation_id"`
	ResultID      string                 `json:"result_id"`
	Processor     string                 `json:"processor"`
	SchemaVersion int                    `json:"schema_version"`
	Value         map[string]interface{} `json:"value"`
	ProducedAt    time.Time              `json:"produced_at"`
}

// ErrorResponse represents the standard error response
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail represents the error detail in the response
type ErrorDetail struct {
	Code        string            `json:"code"`
	Message     string            `json:"message"`
	Details     []ErrorDetailItem `json:"details,omitempty"`
	OperationID string            `json:"operation_id,omitempty"`
	RequestID   string            `json:"request_id"`
	Retryable   bool              `json:"retryable"`
}

// ErrorDetailItem represents a single error detail item
type ErrorDetailItem struct {
	Field  string `json:"field,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// CancelOperationInput represents the input for cancelling an operation
type CancelOperationInput struct {
	Reason string `json:"reason,omitempty"`
}

// RetryOperationInput represents the input for retrying an operation
type RetryOperationInput struct {
	Reason string `json:"reason,omitempty"`
}
