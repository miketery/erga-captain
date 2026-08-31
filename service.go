package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type service struct {
	cfg      config
	store    *store
	hostID   string
	upgrader websocket.Upgrader

	mu          sync.RWMutex
	connections map[string]*executorConn
	recovering  map[string]struct{}
}

func newService(cfg config, store *store, hostID string) *service {
	return &service{
		cfg: cfg, store: store, hostID: hostID,
		upgrader:    websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }},
		connections: make(map[string]*executorConn), recovering: make(map[string]struct{}),
	}
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /websocket/{application}/{token}", s.websocket)
	mux.HandleFunc("GET /v1/executors", s.executors)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", s.workflowStatus)
	mux.HandleFunc("DELETE /v1/workflows/{workflowID}", s.cancelWorkflow)
	return mux
}

func (s *service) runBackground(ctx context.Context) {
	heartbeat := time.NewTicker(time.Second)
	sweep := time.NewTicker(s.cfg.SweepPeriod)
	defer heartbeat.Stop()
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if err := s.store.heartbeatHost(ctx, s.hostID); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("host heartbeat failed", "error", err)
			}
		case <-sweep.C:
			if err := s.recoverySweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("recovery sweep failed", "error", err)
			}
		}
	}
}

func (s *service) recoverySweep(ctx context.Context) error {
	count, err := s.store.expireDisconnected(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		slog.Info("executors declared dead", "count", count)
	}
	dead, err := s.store.deadExecutors(ctx)
	if err != nil {
		return err
	}
	for i := range dead {
		if s.beginRecovery(dead[i].ID) {
			go func(e executor) {
				defer s.endRecovery(e.ID)
				if err := s.recoverExecutor(ctx, e); err != nil && !errors.Is(err, context.Canceled) {
					slog.Warn("executor recovery deferred", "executor_id", e.ID, "error", err)
				}
			}(dead[i])
		}
	}
	return nil
}

func (s *service) beginRecovery(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.recovering[id]; exists {
		return false
	}
	s.recovering[id] = struct{}{}
	return true
}

func (s *service) endRecovery(id string) {
	s.mu.Lock()
	delete(s.recovering, id)
	s.mu.Unlock()
}

func (s *service) recoverExecutor(ctx context.Context, dead executor) error {
	target, err := s.store.matchingHealthy(ctx, dead.ApplicationName, dead.ApplicationVersion, dead.ID)
	if err != nil {
		return err
	}
	if target == nil {
		// Keep the DEAD row. A later sweep will retry when a compatible executor connects.
		return nil
	}

	requestCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	requestID := newID()
	var pending struct {
		wireResponse
		Exist bool `json:"exist"`
	}
	err = s.dispatch(requestCtx, target.ID, map[string]any{
		"type": "exist_pending_workflows", "request_id": requestID,
		"executor_id": dead.ID, "application_version": dead.ApplicationVersion,
	}, requestID, &pending)
	if err != nil {
		return fmt.Errorf("check pending workflows through %s: %w", target.ID, err)
	}
	if !pending.Exist {
		deleted, err := s.store.deleteDeadExecutor(ctx, dead.ID)
		if deleted {
			slog.Info("removed dead executor with no pending workflows", "executor_id", dead.ID)
		}
		return err
	}

	requestID = newID()
	var recovered struct {
		wireResponse
		Success bool `json:"success"`
	}
	err = s.dispatch(requestCtx, target.ID, map[string]any{
		"type": "recovery", "request_id": requestID, "executor_ids": []string{dead.ID},
	}, requestID, &recovered)
	if err != nil {
		return fmt.Errorf("recover through %s: %w", target.ID, err)
	}
	if !recovered.Success {
		return errors.New("executor reported unsuccessful recovery")
	}
	deleted, err := s.store.deleteDeadExecutor(ctx, dead.ID)
	if err != nil {
		return err
	}
	if deleted {
		slog.Info("recovered dead executor workflows", "dead_executor_id", dead.ID,
			"target_executor_id", target.ID)
	}
	return nil
}

func (s *service) dispatch(ctx context.Context, executorID string, request any, requestID string, response any) error {
	s.mu.RLock()
	conn := s.connections[executorID]
	s.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("executor %s is not connected to this host", executorID)
	}
	return conn.dispatch(ctx, request, requestID, response)
}

func (s *service) websocket(w http.ResponseWriter, r *http.Request) {
	application := r.PathValue("application")
	if application == "" {
		http.Error(w, "application is required", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.PathValue("token")), []byte(s.cfg.CaptainKey)) != 1 {
		http.Error(w, "invalid captain key", http.StatusUnauthorized)
		return
	}
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn := newExecutorConn(ws)
	readDone := make(chan error, 1)
	go func() { readDone <- conn.readLoop() }()

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	requestID := newID()
	var response struct {
		wireResponse
		executorInfo
	}
	err = conn.dispatch(ctx, baseMessage{Type: "executor_info", RequestID: requestID}, requestID, &response)
	cancel()
	if err != nil || response.ExecutorID == "" || response.ApplicationVersion == "" {
		slog.Warn("executor handshake failed", "application", application, "error", err)
		_ = conn.close()
		return
	}
	if response.ExecutorMetadata == nil {
		response.ExecutorMetadata = map[string]any{}
	}
	if err := s.store.registerExecutor(r.Context(), application, s.hostID, s.cfg.ExecutorTimeout, response.executorInfo); err != nil {
		slog.Error("executor registration failed", "executor_id", response.ExecutorID, "error", err)
		_ = conn.close()
		return
	}

	s.mu.Lock()
	old := s.connections[response.ExecutorID]
	s.connections[response.ExecutorID] = conn
	s.mu.Unlock()
	if old != nil && old != conn {
		_ = old.close()
	}
	slog.Info("executor connected", "executor_id", response.ExecutorID, "application", application,
		"version", response.ApplicationVersion)

	readErr := <-readDone
	s.mu.Lock()
	if s.connections[response.ExecutorID] == conn {
		delete(s.connections, response.ExecutorID)
		if err := s.store.disconnectExecutor(context.Background(), response.ExecutorID, s.hostID); err != nil {
			slog.Error("mark executor disconnected", "executor_id", response.ExecutorID, "error", err)
		}
	}
	s.mu.Unlock()
	slog.Info("executor disconnected", "executor_id", response.ExecutorID, "error", readErr)
}

func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "host_id": s.hostID})
}

func (s *service) executors(w http.ResponseWriter, r *http.Request) {
	executors, err := s.store.listExecutors(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if executors == nil {
		executors = []executor{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"executors": executors})
}

func (s *service) workflowStatus(w http.ResponseWriter, r *http.Request) {
	application := r.URL.Query().Get("application")
	if application == "" {
		application = "mesa"
	}
	executor, err := s.store.anyHealthy(r.Context(), application)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if executor == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("no healthy executor"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	requestID := newID()
	var response map[string]any
	err = s.dispatch(ctx, executor.ID, map[string]any{
		"type": "get_workflow", "request_id": requestID, "workflow_id": r.PathValue("workflowID"),
		"load_input": false, "load_output": false,
	}, requestID, &response)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *service) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	application := r.URL.Query().Get("application")
	if application == "" {
		application = "mesa"
	}
	executor, err := s.store.anyHealthy(r.Context(), application)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if executor == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("no healthy executor"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	requestID := newID()
	var response map[string]any
	err = s.dispatch(ctx, executor.ID, map[string]any{
		"type": "cancel", "request_id": requestID, "workflow_id": r.PathValue("workflowID"),
		"cancel_children": false,
	}, requestID, &response)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func newID() string {
	// Time plus a monotonic process counter is sufficient for request correlation and
	// host identity in this deliberately single-host prototype.
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strings.ToLower(strconv.FormatUint(nextID(), 36))
}

var idState struct {
	sync.Mutex
	n uint64
}

func nextID() uint64 {
	idState.Lock()
	defer idState.Unlock()
	idState.n++
	return idState.n
}
