package source

import (
	"encoding/json"
	"errors"
	"net/http"

	"assignment/domain"
	"assignment/pipeline"
)

// HTTPHandler is the push-source adapter. It delegates all record processing
// to the same shared pipeline used by pull connectors.
type HTTPHandler struct {
	Topic    string
	Pipeline *pipeline.Pipeline
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.Pipeline == nil {
		http.Error(w, "connector unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	var record domain.Record
	if err := decoder.Decode(&record); err != nil || record == nil {
		http.Error(w, "invalid JSON record", http.StatusBadRequest)
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		http.Error(w, "expected one JSON object", http.StatusBadRequest)
		return
	}
	err := h.Pipeline.Process(r.Context(), h.Topic, record)
	switch {
	case err == nil, errors.Is(err, pipeline.ErrDropped):
		w.WriteHeader(http.StatusAccepted)
	case errors.Is(err, contextCanceledError(r)):
		http.Error(w, "request cancelled", 499)
	default:
		http.Error(w, "record could not be processed; retry may be safe", http.StatusServiceUnavailable)
	}
}

func contextCanceledError(r *http.Request) error { return r.Context().Err() }
