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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

func TestRetryConfiguration(t *testing.T) {
	t.Run("retry_enabled_by_default", func(t *testing.T) {
		props := iceberg.Properties{}
		cfg := table.NewRetryConfig(props)

		assert.Equal(t, 4, cfg.MaxRetries(), "default should be 4 retries")
		assert.Equal(t, 100, cfg.MinRetryWaitMs())
		assert.Equal(t, 60000, cfg.MaxRetryWaitMs())
		assert.Equal(t, 1800000, cfg.TotalRetryTimeMs())
	})

	t.Run("retry_can_be_disabled", func(t *testing.T) {
		props := iceberg.Properties{
			table.CommitNumRetriesKey: "0",
		}
		cfg := table.NewRetryConfig(props)

		assert.Equal(t, 0, cfg.MaxRetries())
	})

	t.Run("retry_configuration_customizable", func(t *testing.T) {
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
}

// TODO: Add integration tests for actual retry behavior with simulated conflicts
// These will be added in PR 4 (Concurrent Append Tests)
