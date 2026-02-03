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

//go:build integration

package sql_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func (s *SQLIntegrationSuite) TestConcurrentAppend() {
	ctx := context.Background()
	namespace := table.Identifier{"concurrent_test"}

	// Create namespace
	err := s.cat.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		_ = err // Might already exist
	}
	defer s.cat.DropNamespace(ctx, namespace)

	// Create table
	tblIdent := table.Identifier{"concurrent_test", "append_test"}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	tbl, err := s.cat.CreateTable(ctx, tblIdent, schema)
	s.Require().NoError(err)
	defer s.cat.DropTable(ctx, tblIdent)

	const numWriters = 5
	const rowsPerWriter = 10

	var wg sync.WaitGroup
	errors := make(chan error, numWriters)
	successCount := make(chan int, numWriters)

	// Launch concurrent writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		writerID := i
		go func() {
			defer wg.Done()

			// Create record batch
			bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
					{Name: "data", Type: arrow.BinaryTypes.String, Nullable: false},
				}, nil))
			defer bldr.Release()

			idBldr := bldr.Field(0).(*array.Int32Builder)
			dataBldr := bldr.Field(1).(*array.StringBuilder)

			for j := 0; j < rowsPerWriter; j++ {
				idBldr.Append(int32(writerID*1000 + j))
				dataBldr.Append(fmt.Sprintf("writer_%d_row_%d", writerID, j))
			}

			rec := bldr.NewRecord()
			defer rec.Release()

			// Start transaction and append
			txn := tbl.NewTransaction()

			reader, err := array.NewRecordReader(rec.Schema(), []arrow.Record{rec})
			if err != nil {
				errors <- fmt.Errorf("writer %d: failed to create reader: %w", writerID, err)
				return
			}

			err = txn.Append(ctx, reader, nil)
			if err != nil {
				errors <- fmt.Errorf("writer %d: failed to append: %w", writerID, err)
				return
			}

			// Commit - this should retry if there's a conflict
			_, err = txn.Commit(ctx)
			if err != nil {
				errors <- fmt.Errorf("writer %d: failed to commit: %w", writerID, err)
				return
			}

			successCount <- 1
		}()
	}

	// Wait for all writers
	wg.Wait()
	close(errors)
	close(successCount)

	// Check for errors
	var allErrors []error
	for err := range errors {
		allErrors = append(allErrors, err)
	}

	// Count successes
	totalSuccess := 0
	for range successCount {
		totalSuccess++
	}

	// With retry logic, all writers should succeed
	s.Require().Empty(allErrors, "all concurrent appends should succeed with retry")
	s.Require().Equal(numWriters, totalSuccess, "all %d writers should succeed", numWriters)

	// Verify data integrity - reload table and check snapshots
	tbl, err = s.cat.LoadTable(ctx, tblIdent)
	s.Require().NoError(err)

	snapshots := tbl.Metadata().Snapshots()
	s.Require().GreaterOrEqual(len(snapshots), numWriters, "should have at least %d snapshots", numWriters)

	// Verify all rows are present
	scan := tbl.Scan()

	tasks, err := scan.PlanFiles(ctx)
	s.Require().NoError(err)

	totalRows := 0
	for _, task := range tasks {
		totalRows += int(task.File.Count())
	}

	expectedRows := numWriters * rowsPerWriter
	s.Require().Equal(expectedRows, totalRows, "should have exactly %d rows", expectedRows)
}

func (s *SQLIntegrationSuite) TestConcurrentAppendWithRetryConfig() {
	ctx := context.Background()
	namespace := table.Identifier{"concurrent_test"}

	// Create namespace
	err := s.cat.CreateNamespace(ctx, namespace, iceberg.Properties{})
	if err != nil {
		_ = err // Might already exist
	}
	defer s.cat.DropNamespace(ctx, namespace)

	// Create table with custom retry configuration
	tblIdent := table.Identifier{"concurrent_test", "retry_config_test"}
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "worker_id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	tbl, err := s.cat.CreateTable(ctx, tblIdent, schema, catalog.WithProperties(iceberg.Properties{
		table.CommitNumRetriesKey:     "8",
		table.CommitMinRetryWaitMsKey: "50",
		table.CommitMaxRetryWaitMsKey: "2000",
	}))
	s.Require().NoError(err)
	defer s.cat.DropTable(ctx, tblIdent)

	const numWriters = 8
	const rowsPerWriter = 5

	var wg sync.WaitGroup
	successes := make([]bool, numWriters)
	var mu sync.Mutex

	// Launch concurrent writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		writerID := i
		go func() {
			defer wg.Done()

			// Create record batch
			bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(
				[]arrow.Field{
					{Name: "worker_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
					{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				}, nil))
			defer bldr.Release()

			workerBldr := bldr.Field(0).(*array.Int32Builder)
			valueBldr := bldr.Field(1).(*array.Int32Builder)

			for j := 0; j < rowsPerWriter; j++ {
				workerBldr.Append(int32(writerID))
				valueBldr.Append(int32(j))
			}

			rec := bldr.NewRecord()
			defer rec.Release()

			txn := tbl.NewTransaction()

			reader, err := array.NewRecordReader(rec.Schema(), []arrow.Record{rec})
			if err != nil {
				return
			}

			if err := txn.Append(ctx, reader, nil); err != nil {
				return
			}

			// Commit with retry
			if _, err := txn.Commit(ctx); err == nil {
				mu.Lock()
				successes[writerID] = true
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Count successful writes
	successCount := 0
	for _, success := range successes {
		if success {
			successCount++
		}
	}

	// With 8 retries, we expect most or all to succeed
	s.Require().GreaterOrEqual(successCount, numWriters-1, "at least %d of %d writers should succeed", numWriters-1, numWriters)

	if successCount == numWriters {
		s.T().Logf("✓ All %d concurrent SQL writers succeeded via retry", numWriters)
	} else {
		s.T().Logf("⚠ %d of %d concurrent SQL writers succeeded", successCount, numWriters)
	}

	// Verify row count
	tbl, err = s.cat.LoadTable(ctx, tblIdent)
	s.Require().NoError(err)

	scan := tbl.Scan()

	tasks, err := scan.PlanFiles(ctx)
	s.Require().NoError(err)

	totalRows := 0
	for _, task := range tasks {
		totalRows += int(task.File.Count())
	}

	expectedRows := successCount * rowsPerWriter
	s.Require().Equal(expectedRows, totalRows, "row count should match successful writers")
}
