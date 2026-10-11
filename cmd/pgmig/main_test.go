package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/m1chlcz/pgmig"
)

func TestRunRejectsUsageErrors(t *testing.T) {
	t.Setenv("PGMIG_DATABASE_URL", "")
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "no command", args: nil, want: "usage:"},
		{name: "unknown command", args: []string{"frobnicate"}, want: "unknown command"},
		{
			name: "missing database url",
			args: []string{"status", "-dir", ".", "-role", "app_owner"},
			want: "database-url",
		},
		{
			name: "missing dir",
			args: []string{"status", "-database-url", "postgres://example", "-role", "app_owner"},
			want: "-dir",
		},
		{
			name: "missing role",
			args: []string{"status", "-database-url", "postgres://example", "-dir", "."},
			want: "-role",
		},
		{
			name: "unexpected arguments",
			args: []string{
				"status", "-database-url", "postgres://example", "-dir", ".", "-role", "app_owner", "extra",
			},
			want: "unexpected",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(testCase.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("exit code = %d, want 2 (stderr: %s)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), testCase.want) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), testCase.want)
			}
		})
	}
}

func TestRunReportsInvalidDatabaseURL(t *testing.T) {
	t.Setenv("PGMIG_DATABASE_URL", "")
	var stdout, stderr bytes.Buffer
	code := run(
		[]string{"status", "-database-url", "://bad", "-dir", ".", "-role", "app_owner"},
		&stdout,
		&stderr,
	)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "database URL is invalid") {
		t.Fatalf("stderr %q does not report the invalid URL", stderr.String())
	}
}

func TestStatusDocumentShape(t *testing.T) {
	appliedAt := time.Date(2026, time.October, 31, 9, 30, 0, 0, time.UTC)
	checksum := [32]byte{0xab}
	statuses := []pgmig.MigrationStatus{
		{
			Version:   1,
			Name:      "alpha",
			State:     pgmig.StateApplied,
			AppliedAt: appliedAt,
			Checksum:  checksum,
		},
		{Version: 2, Name: "beta", State: pgmig.StatePending},
		{Version: 3, Name: "gamma", State: pgmig.StateUnknown, AppliedAt: appliedAt},
	}
	data, err := json.Marshal(statusDocument(statuses))
	if err != nil {
		t.Fatalf("marshal status document: %v", err)
	}
	var decoded []map[string]any
	if unmarshalErr := json.Unmarshal(data, &decoded); unmarshalErr != nil {
		t.Fatalf("unmarshal status document: %v", unmarshalErr)
	}
	if len(decoded) != 3 {
		t.Fatalf("entries = %d, want 3", len(decoded))
	}
	first := decoded[0]
	if first["state"] != "applied" || first["applied_at"] != "2026-10-31T09:30:00Z" {
		t.Fatalf("first entry = %v", first)
	}
	if first["checksum"] != strings.Repeat("ab", 1)+strings.Repeat("00", 31) {
		t.Fatalf("first checksum = %v", first["checksum"])
	}
	second := decoded[1]
	if _, exists := second["applied_at"]; exists {
		t.Fatalf("pending entry must not carry applied_at: %v", second)
	}
	if _, exists := second["checksum"]; exists {
		t.Fatalf("pending entry without checksum must omit the key: %v", second)
	}
	third := decoded[2]
	if third["state"] != "unknown" || third["applied_at"] != "2026-10-31T09:30:00Z" {
		t.Fatalf("third entry = %v", third)
	}
	if _, exists := third["checksum"]; exists {
		t.Fatalf("unknown entry must not carry checksum: %v", third)
	}
}
