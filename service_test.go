package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStandardSDKWebsocketRouteRequiresConfiguredKey(t *testing.T) {
	svc := newService(config{CaptainKey: "expected-key"}, nil, "test-host")

	badRequest := httptest.NewRequest(http.MethodGet, "/websocket/mesa/wrong-key", nil)
	badResponse := httptest.NewRecorder()
	svc.routes().ServeHTTP(badResponse, badRequest)
	if badResponse.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key returned %d, want %d", badResponse.Code, http.StatusUnauthorized)
	}

	validRequest := httptest.NewRequest(http.MethodGet, "/websocket/mesa/expected-key", nil)
	validResponse := httptest.NewRecorder()
	svc.routes().ServeHTTP(validResponse, validRequest)
	if validResponse.Code == http.StatusNotFound || validResponse.Code == http.StatusUnauthorized {
		t.Fatalf("standard SDK route was not matched: status %d", validResponse.Code)
	}
}
