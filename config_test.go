package main

import (
	"testing"
	"time"
)

func TestDurationFromEnv(t *testing.T) {
	t.Setenv("TEST_DURATION", "0.25")
	got, err := durationFromEnv("TEST_DURATION", time.Second)
	if err != nil || got != 250*time.Millisecond {
		t.Fatalf("got %v, %v", got, err)
	}
	t.Setenv("TEST_DURATION", "3s")
	got, err = durationFromEnv("TEST_DURATION", time.Second)
	if err != nil || got != 3*time.Second {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestValidIdentifier(t *testing.T) {
	if !validIdentifier("erga_captain") || validIdentifier("erga-captain") || validIdentifier("") {
		t.Fatal("identifier validation mismatch")
	}
}

func TestLoadConfigUsesCaptainSettings(t *testing.T) {
	t.Setenv("CAPTAIN_ADDR", "127.0.0.1:9090")
	t.Setenv("CAPTAIN_SCHEMA", "captain_test")
	t.Setenv("CAPTAIN_KEY", "test-key")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "127.0.0.1:9090" || cfg.Schema != "captain_test" || cfg.CaptainKey != "test-key" {
		t.Fatalf("captain settings were not loaded: %+v", cfg)
	}
}
