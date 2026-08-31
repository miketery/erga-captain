package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		slog.Error("captain stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := loadDotEnv(); err != nil {
		return err
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := newStore(ctx, cfg.DatabaseURL, cfg.Schema)
	if err != nil {
		return err
	}
	defer st.close()

	hostname, _ := os.Hostname()
	hostID := "host-" + newID()
	if err := st.registerHost(ctx, hostID, hostname, cfg.Addr); err != nil {
		return err
	}
	defer func() {
		if err := st.unregisterHost(context.Background(), hostID); err != nil {
			slog.Error("host unregister failed", "host_id", hostID, "error", err)
		}
	}()

	svc := newService(cfg, st, hostID)
	go svc.runBackground(ctx)
	server := &http.Server{Addr: cfg.Addr, Handler: svc.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("captain listening", "address", cfg.Addr, "schema", cfg.Schema, "host_id", hostID)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func loadDotEnv(filenames ...string) error {
	if err := godotenv.Load(filenames...); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load dotenv: %w", err)
	}
	return nil
}
