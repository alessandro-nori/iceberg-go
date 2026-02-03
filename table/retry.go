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

package table

import (
	"errors"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// retryConfig holds configuration for commit retry behavior
type retryConfig struct {
	maxRetries         int
	minRetryWaitMs     int
	maxRetryWaitMs     int
	totalRetryTimeMs   int
}

// newRetryConfig creates a retryConfig from table properties, using defaults where not specified
func newRetryConfig(props iceberg.Properties) retryConfig {
	return retryConfig{
		maxRetries:       props.GetInt(CommitNumRetriesKey, CommitNumRetriesDefault),
		minRetryWaitMs:   props.GetInt(CommitMinRetryWaitMsKey, CommitMinRetryWaitMsDefault),
		maxRetryWaitMs:   props.GetInt(CommitMaxRetryWaitMsKey, CommitMaxRetryWaitMsDefault),
		totalRetryTimeMs: props.GetInt(CommitTotalRetryTimeMsKey, CommitTotalRetryTimeMsDefault),
	}
}

// exponentialBackoff calculates the backoff duration for a given attempt using exponential backoff
// Formula: min(maxWait, minWait * 2^attempt)
func (rc retryConfig) exponentialBackoff(attempt int) time.Duration {
	// Calculate 2^attempt
	multiplier := 1 << attempt // bit shift is equivalent to 2^attempt

	// Calculate backoff in milliseconds
	backoffMs := rc.minRetryWaitMs * multiplier

	// Cap at maxRetryWaitMs
	if backoffMs > rc.maxRetryWaitMs {
		backoffMs = rc.maxRetryWaitMs
	}

	return time.Duration(backoffMs) * time.Millisecond
}

// isRetriableCommitError determines if an error is a retriable commit conflict
// It detects commit conflicts across different catalog types:
//   - REST: HTTP 409 conflict via "commit failed, refresh and try again" message
//   - SQL: "table has been updated" error message
//   - Glue: AWS SDK ConcurrentModificationException via types.ConcurrentModificationException
func isRetriableCommitError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// REST catalog: HTTP 409 conflict
	// Matches rest.ErrCommitFailed message: "commit failed, refresh and try again"
	if strings.Contains(errStr, "commit failed") && strings.Contains(errStr, "refresh and try again") {
		return true
	}

	// SQL catalog: check for concurrent modification message
	if strings.Contains(errStr, "table has been updated") {
		return true
	}

	// Glue catalog: check for ConcurrentModificationException
	var concurrentModErr *types.ConcurrentModificationException
	if errors.As(err, &concurrentModErr) {
		return true
	}

	return false
}
