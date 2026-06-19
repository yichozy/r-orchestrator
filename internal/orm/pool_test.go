package orm

import (
	"strings"
	"testing"
)

func TestGetDBRejectsUninitializedDefaultDB(t *testing.T) {
	default_db = nil

	_, err := GetDB()
	if err == nil {
		t.Fatalf("expected uninitialized orm db to fail")
	}
	if !strings.Contains(err.Error(), "orm db is not initialized") {
		t.Fatalf("expected initialization context in error, got %q", err)
	}
}

func TestOpenRejectsInvalidDatabaseURL(t *testing.T) {
	default_db = nil

	_, err := Open("postgres://%")
	if err == nil {
		t.Fatalf("expected invalid database url to fail")
	}
	if !strings.Contains(err.Error(), "open gorm postgres connection") {
		t.Fatalf("expected open context in error, got %q", err)
	}

	_, getErr := GetDB()
	if getErr == nil {
		t.Fatalf("expected default orm db to remain uninitialized after failed open")
	}
}
