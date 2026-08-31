package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type config struct {
	Addr            string
	DatabaseURL     string
	Schema          string
	CaptainKey      string
	ExecutorTimeout time.Duration
	SweepPeriod     time.Duration
	RequestTimeout  time.Duration
}

func loadConfig() (config, error) {
	c := config{
		Addr:            envOr("CAPTAIN_ADDR", "127.0.0.1:8080"),
		DatabaseURL:     envOr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/erga_captain?sslmode=disable"),
		Schema:          envOr("CAPTAIN_SCHEMA", "erga_captain"),
		CaptainKey:      envOr("CAPTAIN_KEY", "local-dev-key"),
		ExecutorTimeout: 2 * time.Second,
		SweepPeriod:     500 * time.Millisecond,
		RequestTimeout:  5 * time.Second,
	}

	var err error
	if c.ExecutorTimeout, err = durationFromEnv("EXECUTOR_TIMEOUT", c.ExecutorTimeout); err != nil {
		return config{}, err
	}
	if c.SweepPeriod, err = durationFromEnv("RECOVERY_SWEEP_PERIOD", c.SweepPeriod); err != nil {
		return config{}, err
	}
	if c.RequestTimeout, err = durationFromEnv("REQUEST_TIMEOUT", c.RequestTimeout); err != nil {
		return config{}, err
	}
	if !validIdentifier(c.Schema) {
		return config{}, fmt.Errorf("CAPTAIN_SCHEMA must contain only letters, digits, and underscores")
	}
	return c, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil {
		if seconds <= 0 {
			return 0, fmt.Errorf("%s must be positive", name)
		}
		return time.Duration(seconds * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration or number of seconds", name)
	}
	return d, nil
}

func validIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}
