package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

type baseMessage struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
}

type wireResponse struct {
	baseMessage
	ErrorMessage *string `json:"error_message,omitempty"`
}

type executorConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[string]chan []byte
	done    chan struct{}
}

func newExecutorConn(ws *websocket.Conn) *executorConn {
	return &executorConn{ws: ws, pending: make(map[string]chan []byte), done: make(chan struct{})}
}

func (c *executorConn) readLoop() error {
	defer close(c.done)
	for {
		_, payload, err := c.ws.ReadMessage()
		if err != nil {
			c.failPending()
			return err
		}
		var base baseMessage
		if err := json.Unmarshal(payload, &base); err != nil || base.RequestID == "" {
			continue
		}
		c.mu.Lock()
		response := c.pending[base.RequestID]
		if response != nil {
			delete(c.pending, base.RequestID)
		}
		c.mu.Unlock()
		if response != nil {
			response <- payload
		}
	}
}

func (c *executorConn) failPending() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, response := range c.pending {
		delete(c.pending, id)
		close(response)
	}
}

func (c *executorConn) dispatch(ctx context.Context, request any, requestID string, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	result := make(chan []byte, 1)
	c.mu.Lock()
	c.pending[requestID] = result
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
	}()

	c.writeMu.Lock()
	err = c.ws.WriteMessage(websocket.TextMessage, payload)
	c.writeMu.Unlock()
	if err != nil {
		return err
	}

	select {
	case raw, ok := <-result:
		if !ok {
			return errors.New("executor disconnected")
		}
		var base wireResponse
		if err := json.Unmarshal(raw, &base); err != nil {
			return fmt.Errorf("decode executor response: %w", err)
		}
		if base.ErrorMessage != nil {
			return errors.New(*base.ErrorMessage)
		}
		if err := json.Unmarshal(raw, response); err != nil {
			return fmt.Errorf("decode executor response: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("executor disconnected")
	}
}

func (c *executorConn) close() error {
	return c.ws.Close()
}
