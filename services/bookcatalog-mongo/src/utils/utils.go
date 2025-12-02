package utils

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// ParseBody decodes a JSON request body into dst.
func ParseBody(r *http.Request, dst interface{}) error {
	if dst == nil {
		return errors.New("destination cannot be nil")
	}
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is empty")
		}
		return err
	}

	return nil
}

// WriteJSON serializes payload as JSON with the provided HTTP status code.
func WriteJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}

// WriteError sends a standardized JSON error response.
func WriteError(w http.ResponseWriter, status int, err error) {
	type errorResponse struct {
		Error string `json:"error"`
	}
	WriteJSON(w, status, errorResponse{Error: err.Error()})
}
