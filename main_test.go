package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotEnv(t *testing.T) {
	const loadedName = "CAPTAIN_DOTENV_TEST_LOADED"
	const existingName = "CAPTAIN_DOTENV_TEST_EXISTING"

	t.Setenv(loadedName, "")
	if err := os.Unsetenv(loadedName); err != nil {
		t.Fatal(err)
	}
	t.Setenv(existingName, "from-process")

	path := filepath.Join(t.TempDir(), ".env")
	contents := loadedName + "=from-file\n" + existingName + "=from-file\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadDotEnv(path); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv(loadedName); got != "from-file" {
		t.Fatalf("loaded value = %q, want %q", got, "from-file")
	}
	if got := os.Getenv(existingName); got != "from-process" {
		t.Fatalf("existing value = %q, want %q", got, "from-process")
	}
}

func TestLoadDotEnvAllowsMissingFile(t *testing.T) {
	if err := loadDotEnv(filepath.Join(t.TempDir(), "missing.env")); err != nil {
		t.Fatal(err)
	}
}
