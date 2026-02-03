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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
)

func TestNewRetryConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		props := iceberg.Properties{}
		cfg := table.NewRetryConfig(props)

		assert.Equal(t, table.CommitNumRetriesDefault, cfg.MaxRetries())
		assert.Equal(t, table.CommitMinRetryWaitMsDefault, cfg.MinRetryWaitMs())
		assert.Equal(t, table.CommitMaxRetryWaitMsDefault, cfg.MaxRetryWaitMs())
		assert.Equal(t, table.CommitTotalRetryTimeMsDefault, cfg.TotalRetryTimeMs())
	})

	t.Run("custom_values", func(t *testing.T) {
		props := iceberg.Properties{
			table.CommitNumRetriesKey:         "10",
			table.CommitMinRetryWaitMsKey:     "200",
			table.CommitMaxRetryWaitMsKey:     "30000",
			table.CommitTotalRetryTimeMsKey:   "600000",
		}
		cfg := table.NewRetryConfig(props)

		assert.Equal(t, 10, cfg.MaxRetries())
		assert.Equal(t, 200, cfg.MinRetryWaitMs())
		assert.Equal(t, 30000, cfg.MaxRetryWaitMs())
		assert.Equal(t, 600000, cfg.TotalRetryTimeMs())
	})

	t.Run("disable_retry", func(t *testing.T) {
		props := iceberg.Properties{
			table.CommitNumRetriesKey: "0",
		}
		cfg := table.NewRetryConfig(props)

		assert.Equal(t, 0, cfg.MaxRetries())
	})
}

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		name           string
		minWaitMs      int
		maxWaitMs      int
		attempt        int
		expectedMs     int
	}{
		{
			name:       "attempt_0",
			minWaitMs:  100,
			maxWaitMs:  60000,
			attempt:    0,
			expectedMs: 100, // 100 * 2^0 = 100
		},
		{
			name:       "attempt_1",
			minWaitMs:  100,
			maxWaitMs:  60000,
			attempt:    1,
			expectedMs: 200, // 100 * 2^1 = 200
		},
		{
			name:       "attempt_2",
			minWaitMs:  100,
			maxWaitMs:  60000,
			attempt:    2,
			expectedMs: 400, // 100 * 2^2 = 400
		},
		{
			name:       "attempt_3",
			minWaitMs:  100,
			maxWaitMs:  60000,
			attempt:    3,
			expectedMs: 800, // 100 * 2^3 = 800
		},
		{
			name:       "capped_at_max",
			minWaitMs:  100,
			maxWaitMs:  1000,
			attempt:    10,
			expectedMs: 1000, // 100 * 2^10 = 102400, but capped at 1000
		},
		{
			name:       "custom_min_wait",
			minWaitMs:  250,
			maxWaitMs:  60000,
			attempt:    2,
			expectedMs: 1000, // 250 * 2^2 = 1000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props := iceberg.Properties{
				table.CommitMinRetryWaitMsKey: fmt.Sprintf("%d", tt.minWaitMs),
				table.CommitMaxRetryWaitMsKey: fmt.Sprintf("%d", tt.maxWaitMs),
			}
			cfg := table.NewRetryConfig(props)

			duration := cfg.ExponentialBackoff(tt.attempt)
			expectedDuration := time.Duration(tt.expectedMs) * time.Millisecond

			assert.Equal(t, expectedDuration, duration,
				"expected %dms but got %dms", tt.expectedMs, duration.Milliseconds())
		})
	}
}

func TestIsRetriableCommitError(t *testing.T) {
	t.Run("nil_error", func(t *testing.T) {
		assert.False(t, table.IsRetriableCommitError(nil))
	})

	t.Run("rest_catalog_409_conflict", func(t *testing.T) {
		// Simulates rest.ErrCommitFailed message
		err := errors.New("commit failed, refresh and try again")
		assert.True(t, table.IsRetriableCommitError(err))
	})

	t.Run("rest_catalog_wrapped_409", func(t *testing.T) {
		// Simulates wrapped rest.ErrCommitFailed
		err := fmt.Errorf("failed to commit: commit failed, refresh and try again")
		assert.True(t, table.IsRetriableCommitError(err))
	})

	t.Run("sql_catalog_concurrent_update", func(t *testing.T) {
		err := errors.New("table has been updated by another process: db.table")
		assert.True(t, table.IsRetriableCommitError(err))
	})

	t.Run("sql_catalog_wrapped_concurrent_update", func(t *testing.T) {
		err := fmt.Errorf("commit failed: %w", errors.New("table has been updated"))
		assert.True(t, table.IsRetriableCommitError(err))
	})

	t.Run("glue_catalog_concurrent_modification", func(t *testing.T) {
		glueErr := &types.ConcurrentModificationException{
			Message: aws.String("Table version mismatch"),
		}
		assert.True(t, table.IsRetriableCommitError(glueErr))
	})

	t.Run("glue_catalog_wrapped_concurrent_modification", func(t *testing.T) {
		glueErr := &types.ConcurrentModificationException{
			Message: aws.String("Version conflict"),
		}
		wrappedErr := fmt.Errorf("update failed: %w", glueErr)
		assert.True(t, table.IsRetriableCommitError(wrappedErr))
	})

	t.Run("non_retriable_rest_error", func(t *testing.T) {
		// Simulates a non-retriable REST error
		err := errors.New("bad request")
		assert.False(t, table.IsRetriableCommitError(err))
	})

	t.Run("non_retriable_sql_error", func(t *testing.T) {
		err := errors.New("syntax error in SQL")
		assert.False(t, table.IsRetriableCommitError(err))
	})

	t.Run("non_retriable_glue_error", func(t *testing.T) {
		glueErr := &types.EntityNotFoundException{
			Message: aws.String("Table not found"),
		}
		assert.False(t, table.IsRetriableCommitError(glueErr))
	})

	t.Run("generic_error", func(t *testing.T) {
		err := errors.New("some other error")
		assert.False(t, table.IsRetriableCommitError(err))
	})

	t.Run("validation_error", func(t *testing.T) {
		err := errors.New("invalid schema")
		assert.False(t, table.IsRetriableCommitError(err))
	})
}
