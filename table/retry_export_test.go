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
	"time"

	"github.com/apache/iceberg-go"
)

// Export internal functions and types for testing

// RetryConfig is an exported wrapper for retryConfig
type RetryConfig struct {
	rc retryConfig
}

// NewRetryConfig creates a RetryConfig from properties for testing
func NewRetryConfig(props iceberg.Properties) RetryConfig {
	return RetryConfig{rc: newRetryConfig(props)}
}

// MaxRetries returns the max number of retries
func (r RetryConfig) MaxRetries() int {
	return r.rc.maxRetries
}

// MinRetryWaitMs returns the minimum retry wait time in milliseconds
func (r RetryConfig) MinRetryWaitMs() int {
	return r.rc.minRetryWaitMs
}

// MaxRetryWaitMs returns the maximum retry wait time in milliseconds
func (r RetryConfig) MaxRetryWaitMs() int {
	return r.rc.maxRetryWaitMs
}

// TotalRetryTimeMs returns the total retry time budget in milliseconds
func (r RetryConfig) TotalRetryTimeMs() int {
	return r.rc.totalRetryTimeMs
}

// ExponentialBackoff calculates backoff for testing
func (r RetryConfig) ExponentialBackoff(attempt int) time.Duration {
	return r.rc.exponentialBackoff(attempt)
}

// IsRetriableCommitError is an exported wrapper for isRetriableCommitError
func IsRetriableCommitError(err error) bool {
	return isRetriableCommitError(err)
}
