package app

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/gin-gonic/gin"
)

const RequestIDKey = "X-Request-ID"

func CorrelationMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(RequestIDKey)
		if requestID == "" {
			requestID = generateRequestID()
		}
		c.Header(RequestIDKey, requestID)
		c.Set("request_id", requestID)
		c.Next()
	}
}

func generateRequestID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

func GetRequestID(c *gin.Context) string {
	if id, exists := c.Get("request_id"); exists {
		return id.(string)
	}
	return ""
}

func ErrorMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) > 0 {
			err := c.Errors.Last()
			requestID := GetRequestID(c)

			// Don't overwrite response if already written
			if c.Writer.Status() == http.StatusOK || c.Writer.Status() == http.StatusAccepted {
				status := http.StatusInternalServerError
				errorCode := "INTERNAL_ERROR"

				switch {
				case c.Writer.Status() == http.StatusNotFound:
					status = http.StatusNotFound
					errorCode = "OPERATION_NOT_FOUND"
				case c.Writer.Status() == http.StatusBadRequest:
					status = http.StatusBadRequest
					errorCode = "VALIDATION_ERROR"
				case c.Writer.Status() == http.StatusConflict:
					status = http.StatusConflict
					errorCode = "OPERATION_CONFLICT"
				}

				c.JSON(status, gin.H{
					"error": gin.H{
						"code":       errorCode,
						"message":    err.Error(),
						"request_id": requestID,
						"retryable":  false,
					},
				})
			}
		}
	}
}

func SanitizeErrorMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		if len(c.Errors) > 0 {
			requestID := GetRequestID(c)

			// Return sanitized error to client
			if c.Writer.Status() >= http.StatusInternalServerError {
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL_ERROR",
						"message":    "An unexpected error occurred",
						"request_id": requestID,
						"retryable":  true,
					},
				})
			}
		}
	}
}

func RecoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				requestID := GetRequestID(c)
				c.JSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"code":       "INTERNAL_ERROR",
						"message":    "An unexpected error occurred",
						"request_id": requestID,
						"retryable":  true,
					},
				})
				c.Abort()
			}
		}()
		c.Next()
	}
}

func CORSMiddleware(allowedOrigins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		allowed := false
		for _, allowedOrigin := range allowedOrigins {
			if origin == allowedOrigin || allowedOrigin == "*" {
				allowed = true
				break
			}
		}

		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key, If-Match, Digest, X-File-Size, X-File-Name, X-Request-ID")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			c.Header("Access-Control-Max-Age", "86400")
		}

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func ContentTypeMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "POST" || c.Request.Method == "PUT" {
			contentType := c.ContentType()
			if contentType != "application/json" && contentType != "application/octet-stream" && contentType != "multipart/form-data" {
				c.JSON(http.StatusUnsupportedMediaType, gin.H{
					"error": gin.H{
						"code":       "VALIDATION_ERROR",
						"message":    "Content-Type must be application/json or application/octet-stream",
						"request_id": GetRequestID(c),
						"retryable":  false,
					},
				})
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// authHeader := c.GetHeader("Authorization")
		// if authHeader == "" {
		// 	c.JSON(http.StatusUnauthorized, gin.H{
		// 		"error": gin.H{
		// 			"code":       "AUTHENTICATION_REQUIRED",
		// 			"message":    "Authorization header is required",
		// 			"request_id": GetRequestID(c),
		// 			"retryable":  false,
		// 		},
		// 	})
		// 	c.Abort()
		// 	return
		// }

		// if !strings.HasPrefix(authHeader, "Bearer ") && !strings.HasPrefix(authHeader, "Basic ") {
		// 	c.JSON(http.StatusUnauthorized, gin.H{
		// 		"error": gin.H{
		// 			"code":       "AUTHENTICATION_REQUIRED",
		// 			"message":    "Invalid Authorization header format",
		// 			"request_id": GetRequestID(c),
		// 			"retryable":  false,
		// 		},
		// 	})
		// 	c.Abort()
		// 	return
		// }

		c.Next()
	}
}

func RequestSizeLimitMiddleware(maxSize int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxSize {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": gin.H{
					"code":       "CONTENT_TOO_LARGE",
					"message":    "Request body exceeds maximum allowed size",
					"request_id": GetRequestID(c),
					"retryable":  false,
				},
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
