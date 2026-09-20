package app

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
)

type envelope struct {
	Data any `json:"data"`
}

var Validate *validator.Validate

func init() {
	Validate = validator.New(validator.WithRequiredStructEnabled())
}

func WriteJSON(c *gin.Context, status int, data any) error {
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Status(status)
	return json.NewEncoder(c.Writer).Encode(data)
}

func ReadJSON(c *gin.Context, data any) error {
	maxBytes := 1_048_576
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, int64(maxBytes))
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(data)
}

func WriteJSONError(c *gin.Context, status int, err string) error {
	type envelope struct {
		Error string `json:"error"`
	}
	return WriteJSON(c, status, &envelope{Error: err})
}

func (app *Application) jsonResponse(c *gin.Context, status int, data any) error {

	return WriteJSON(c, status, &envelope{Data: data})
}
