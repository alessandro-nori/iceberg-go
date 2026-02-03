// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

func TestRetriableErrorDetection(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			name:      "nil_error",
			err:       nil,
			retriable: false,
		},
		{
			name:      "rest_commit_failed",
			err:       errors.New("rest error: commit failed, refresh and try again"),
			retriable: true,
		},
		{
			name:      "rest_commit_failed_wrapped",
			err:       fmt.Errorf("transaction failed: %w", errors.New("commit failed, refresh and try again")),
			retriable: true,
		},
		{
			name:      "sql_table_updated",
			err:       errors.New("table has been updated by another process: db.table"),
			retriable: true,
		},
		{
			name:      "sql_table_updated_wrapped",
			err:       fmt.Errorf("commit error: %w", errors.New("table has been updated")),
			retriable: true,
		},
		{
			name:      "generic_error",
			err:       errors.New("some other error"),
			retriable: false,
		},
		{
			name:      "validation_error",
			err:       errors.New("invalid schema"),
			retriable: false,
		},
		{
			name:      "not_found_error",
			err:       errors.New("table not found"),
			retriable: false,
		},
		{
			name:      "auth_error",
			err:       errors.New("unauthorized"),
			retriable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := table.IsRetriableCommitError(tt.err)
			assert.Equal(t, tt.retriable, result, "error: %v", tt.err)
		})
	}
}

func TestRetryBackoffProgression(t *testing.T) {
	props := map[string]string{
		table.CommitMinRetryWaitMsKey: "100",
		table.CommitMaxRetryWaitMsKey: "10000",
	}
	cfg := table.NewRetryConfig(props)

	tests := []struct {
		attempt    int
		expectedMs int64
	}{
		{attempt: 0, expectedMs: 100},   // 100 * 2^0 = 100
		{attempt: 1, expectedMs: 200},   // 100 * 2^1 = 200
		{attempt: 2, expectedMs: 400},   // 100 * 2^2 = 400
		{attempt: 3, expectedMs: 800},   // 100 * 2^3 = 800
		{attempt: 4, expectedMs: 1600},  // 100 * 2^4 = 1600
		{attempt: 5, expectedMs: 3200},  // 100 * 2^5 = 3200
		{attempt: 6, expectedMs: 6400},  // 100 * 2^6 = 6400
		{attempt: 7, expectedMs: 10000}, // 100 * 2^7 = 12800, capped at 10000
		{attempt: 8, expectedMs: 10000}, // Capped
		{attempt: 10, expectedMs: 10000}, // Capped
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			backoff := cfg.ExponentialBackoff(tt.attempt)
			assert.Equal(t, tt.expectedMs, backoff.Milliseconds(),
				"attempt %d should have backoff %dms", tt.attempt, tt.expectedMs)
		})
	}
}

func TestRetryConfigEdgeCases(t *testing.T) {
	t.Run("zero_retries_disables_retry", func(t *testing.T) {
		props := map[string]string{
			table.CommitNumRetriesKey: "0",
		}
		cfg := table.NewRetryConfig(props)
		assert.Equal(t, 0, cfg.MaxRetries())
	})

	t.Run("large_retry_count", func(t *testing.T) {
		props := map[string]string{
			table.CommitNumRetriesKey: "100",
		}
		cfg := table.NewRetryConfig(props)
		assert.Equal(t, 100, cfg.MaxRetries())
	})

	t.Run("very_short_timeouts", func(t *testing.T) {
		props := map[string]string{
			table.CommitMinRetryWaitMsKey:   "1",
			table.CommitMaxRetryWaitMsKey:   "10",
			table.CommitTotalRetryTimeMsKey: "100",
		}
		cfg := table.NewRetryConfig(props)
		assert.Equal(t, 1, cfg.MinRetryWaitMs())
		assert.Equal(t, 10, cfg.MaxRetryWaitMs())
		assert.Equal(t, 100, cfg.TotalRetryTimeMs())
	})

	t.Run("min_equals_max", func(t *testing.T) {
		props := map[string]string{
			table.CommitMinRetryWaitMsKey: "1000",
			table.CommitMaxRetryWaitMsKey: "1000",
		}
		cfg := table.NewRetryConfig(props)

		// All backoffs should be capped at 1000ms
		for attempt := 0; attempt < 10; attempt++ {
			backoff := cfg.ExponentialBackoff(attempt)
			assert.LessOrEqual(t, backoff.Milliseconds(), int64(1000),
				"attempt %d should not exceed max", attempt)
		}
	})
}

func TestRetryErrorMessagePatterns(t *testing.T) {
	// Test various error message patterns that should be detected
	retriablePatterns := []string{
		"commit failed, refresh and try again",
		"REST error: commit failed, refresh and try again",
		"table has been updated by another process",
		"database error: table has been updated",
		"commit failed, refresh and try again: details",
		"wrapped: commit failed, refresh and try again",
	}

	for _, pattern := range retriablePatterns {
		t.Run(pattern, func(t *testing.T) {
			err := errors.New(pattern)
			assert.True(t, table.IsRetriableCommitError(err),
				"pattern should be detected as retriable: %s", pattern)
		})
	}

	// Test non-retriable patterns
	nonRetriablePatterns := []string{
		"invalid table",
		"schema mismatch",
		"unauthorized access",
		"table not found",
		"network timeout",
		"connection refused",
	}

	for _, pattern := range nonRetriablePatterns {
		t.Run(pattern, func(t *testing.T) {
			err := errors.New(pattern)
			assert.False(t, table.IsRetriableCommitError(err),
				"pattern should not be detected as retriable: %s", pattern)
		})
	}
}
