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

use crate::bucketing::BucketingFunction;
use crate::client::table::partition_getter::{PartitionGetter, get_physical_path};
use crate::client::table::{PreparedLogBatch, PreparedLogBatchResult, RecoverableWriteMode};
use crate::client::write::encode_prepared_arrow_batch;
use crate::client::{WriteRecord, WriteResultFuture, WriterClient};
use crate::error::Error::{IllegalArgument, UnexpectedError};
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::record::prepare_append_record_batch;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{RecordBatch, UInt32Array};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct TableAppend {
    table_path: Arc<TablePath>,
    table_info: Arc<TableInfo>,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub(super) fn new(
        table_path: TablePath,
        table_info: Arc<TableInfo>,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path: Arc::new(table_path),
            table_info,
            writer_client,
        }
    }

    pub fn create_writer(&self) -> Result<AppendWriter> {
        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                self.table_info.row_type(),
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let bucket_router = if self.table_info.has_bucket_key() {
            let data_lake_format = self.table_info.table_config.get_datalake_format()?;
            let encoder = KeyEncoderFactory::of_bucket_key_encoder(
                self.table_info.row_type(),
                self.table_info.get_bucket_keys(),
                &data_lake_format,
            )?;
            Some(BucketRouter {
                encoder: Mutex::new(encoder),
                bucketing: <dyn BucketingFunction>::of(data_lake_format.as_ref()),
            })
        } else {
            None
        };

        Ok(AppendWriter {
            table_path: Arc::clone(&self.table_path),
            partition_getter,
            writer_client: self.writer_client.clone(),
            table_info: Arc::clone(&self.table_info),
            bucket_router,
        })
    }
}

struct BucketRouter {
    encoder: Mutex<Box<dyn KeyEncoder>>,
    bucketing: Box<dyn BucketingFunction>,
}

impl BucketRouter {
    fn encode_key(&self, row: &dyn InternalRow) -> Result<Bytes> {
        self.encoder.lock().encode_key(row)
    }

    fn bucket_of(&self, row: &dyn InternalRow, num_buckets: i32) -> Result<(i32, Bytes)> {
        let key = self.encode_key(row)?;
        let bucket = self.bucketing.bucketing(&key, num_buckets)?;
        Ok((bucket, key))
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    partition_getter: Option<PartitionGetter>,
    writer_client: Arc<WriterClient>,
    table_info: Arc<TableInfo>,
    bucket_router: Option<BucketRouter>,
}

impl AppendWriter {
    /// Prepares one caller-selected bucket from an Arrow batch into immutable restartable state.
    ///
    /// All rows must route to `bucket_id` and, for partitioned tables, to the same physical
    /// partition. The caller must use a stable, unique nonnegative writer id and durably manage
    /// the next per-bucket sequence; this API does not coordinate writer ids across processes.
    pub async fn prepare_log_batch(
        &self,
        batch: RecordBatch,
        bucket_id: i32,
        writer_id: i64,
        batch_sequence: i32,
    ) -> Result<PreparedLogBatch> {
        if batch.num_rows() == 0 {
            return Err(IllegalArgument {
                message: "cannot prepare an empty log batch".to_string(),
            });
        }
        let batch = prepare_append_record_batch(&batch, self.table_info.row_type())?;
        let first_row = ColumnarRow::new(
            Arc::new(batch.clone()),
            Arc::new(self.table_info.row_type.clone()),
            0,
            None,
        )?;
        let physical_path = Arc::new(get_physical_path(
            &self.table_path,
            self.partition_getter.as_ref(),
            &first_row,
        )?);
        let routing_bucket_count = self
            .writer_client
            .routing_bucket_count(&physical_path, &self.table_info)?;
        if bucket_id < 0 || bucket_id >= routing_bucket_count {
            return Err(IllegalArgument {
                message: format!(
                    "bucket {bucket_id} is outside routing count {routing_bucket_count}"
                ),
            });
        }
        let batch_arc = Arc::new(batch.clone());
        let row_type = Arc::new(self.table_info.row_type.clone());
        let mut row = ColumnarRow::new(batch_arc, row_type, 0, None)?;
        for index in 0..batch.num_rows() {
            row.set_row_id(index);
            if self.partition_getter.is_some()
                && get_physical_path(&self.table_path, self.partition_getter.as_ref(), &row)?
                    != *physical_path
            {
                return Err(IllegalArgument {
                    message: "prepared rows span physical partitions".to_string(),
                });
            }
            if let Some(router) = &self.bucket_router {
                if router.bucket_of(&row, routing_bucket_count)?.0 != bucket_id {
                    return Err(IllegalArgument {
                        message: "prepared rows span buckets or do not match bucket_id".to_string(),
                    });
                }
            }
        }
        let records = encode_prepared_arrow_batch(
            &self.table_info,
            Arc::clone(&physical_path),
            Arc::new(batch.clone()),
            writer_id,
            batch_sequence,
        )?;
        let partition_id = if physical_path.get_partition_name().is_some() {
            Some(
                self.writer_client
                    .resolve_partition_id(&physical_path)
                    .await?
                    .ok_or_else(|| IllegalArgument {
                        message: "prepared physical partition is unknown".to_string(),
                    })?,
            )
        } else {
            None
        };
        PreparedLogBatch::new(
            self.table_info.table_id,
            partition_id,
            bucket_id,
            writer_id,
            batch_sequence,
            records,
        )
    }

    /// Sends an immutable prepared batch using the explicit recoverable protocol mode.
    ///
    /// # Caller-managed durability
    ///
    /// Persist `batch.encode()` durably before the first network dispatch. Persist a `sending`
    /// state transition before invoking [`RecoverableWriteMode::NewSend`]. After any attempt
    /// that may have reached the server—including a lost response, timeout, or process crash—
    /// restore the same state and invoke [`RecoverableWriteMode::Retry`] with the exact same
    /// writer id, sequence, target, and bytes. Do not make a new batch or use `NewSend` again
    /// while the outcome is uncertain. Only after this method returns `Ok` should the caller
    /// durably persist the acknowledgment and advance its sequence/checkpoint. A returned error
    /// is not an acknowledgment; the caller must retain the prepared state and choose whether to
    /// retry or handle the typed error.
    ///
    /// Keep at most one uncertain prepared batch per writer and bucket. The server's duplicate
    /// recognition is bounded (currently five recent sequences); this is neither a transaction
    /// nor indefinite deduplication, and it does not coordinate caller checkpoint storage with
    /// Fluss. The versioned encoding uses CRC32C for accidental-corruption detection, not
    /// authentication. Treat its bytes as a Rust SDK persistence format, not a wire protocol.
    ///
    /// This method does not retry transport failures automatically. The prepared state is bound
    /// to a table id and physical bucket; it is not rerouted. A peer that negotiated a ProduceLog
    /// version below 2 is rejected before the prepared request is transmitted.
    pub async fn send_prepared_log_batch(
        &self,
        batch: &PreparedLogBatch,
        mode: RecoverableWriteMode,
    ) -> Result<PreparedLogBatchResult> {
        if batch.table_id() != self.table_info.table_id
            || i32::from(batch.schema_id()) != self.table_info.schema_id
        {
            return Err(IllegalArgument {
                message: "prepared batch table or schema does not match this writer".to_string(),
            });
        }
        let response = self
            .writer_client
            .send_prepared_log_batch(&self.table_path, batch, mode)
            .await?;
        PreparedLogBatch::result_from_response(response, batch.partition_id(), batch.bucket_id())
    }

    fn check_field_count<R: InternalRow>(&self, row: &R) -> Result<()> {
        let expected = self.table_info.get_row_type().fields().len();
        if row.get_field_count() != expected {
            return Err(IllegalArgument {
                message: format!(
                    "The field count of the row does not match the table schema. \
                     Expected: {}, Actual: {}",
                    expected,
                    row.get_field_count()
                ),
            });
        }
        Ok(())
    }

    /// Appends a row to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// # Arguments
    /// * row - the row to append.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append<R: InternalRow>(&self, row: &R) -> Result<WriteResultFuture> {
        self.append_with_deadline(row, None)
    }

    /// Internal: bounds the buffer-memory wait by `deadline` so a language binding can share
    /// one `client.writer.buffer.wait-timeout` budget with callback admission waits. Not a
    /// public deadline API; use [`Self::append`], which passes `None` and defers to the config.
    #[doc(hidden)]
    pub fn append_with_deadline<R: InternalRow>(
        &self,
        row: &R,
        deadline: Option<Instant>,
    ) -> Result<WriteResultFuture> {
        self.check_field_count(row)?;
        let physical_table_path = Arc::new(get_physical_path(
            &self.table_path,
            self.partition_getter.as_ref(),
            row,
        )?);
        let bucket_key = match &self.bucket_router {
            Some(router) => Some(router.encode_key(row)?),
            None => None,
        };
        let record = WriteRecord::for_append(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            row,
        )
        .with_bucket_key(bucket_key)
        .with_submit_deadline(deadline);
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Appends an Arrow RecordBatch to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// For a partitioned table the partition is derived from the **first row**, so all rows
    /// in the batch must belong to the same partition. Rows are distributed across buckets by
    /// their bucket key automatically.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append_arrow_batch(&self, batch: RecordBatch) -> Result<WriteResultFuture> {
        self.append_arrow_batch_with_deadline(batch, None)
    }

    /// Internal: bounds the buffer-memory wait by `deadline` so a language binding can share
    /// one `client.writer.buffer.wait-timeout` budget with callback admission waits. Not a
    /// public deadline API; use [`Self::append_arrow_batch`], which passes `None`.
    #[doc(hidden)]
    pub fn append_arrow_batch_with_deadline(
        &self,
        batch: RecordBatch,
        deadline: Option<Instant>,
    ) -> Result<WriteResultFuture> {
        if batch.num_rows() == 0 {
            // Nothing to write; also avoids a keyless send to a bucket-key table.
            return Ok(WriteResultFuture::join(Vec::new()));
        }
        let batch = prepare_append_record_batch(&batch, self.table_info.row_type())?;
        let physical_table_path = if self.partition_getter.is_some() {
            let first_row = ColumnarRow::new(
                Arc::new(batch.clone()),
                Arc::new(self.table_info.row_type.clone()),
                0,
                None,
            )?;
            Arc::new(get_physical_path(
                &self.table_path,
                self.partition_getter.as_ref(),
                &first_row,
            )?)
        } else {
            Arc::new(PhysicalTablePath::of(Arc::clone(&self.table_path)))
        };

        let Some(router) = self.bucket_router.as_ref() else {
            return self.send_arrow_batch(batch, physical_table_path, None, None, deadline);
        };

        // Group rows by bucket, keeping one key per bucket (it hashes back there).
        let num_buckets = self
            .writer_client
            .routing_bucket_count(&physical_table_path, &self.table_info)?;
        let batch_arc = Arc::new(batch.clone());
        let row_type = Arc::new(self.table_info.row_type.clone());
        let mut groups: HashMap<i32, (Vec<u32>, Bytes)> = HashMap::new();
        // Reuse one row view; ColumnarRow::new re-downcasts every column.
        let mut row = ColumnarRow::new(Arc::clone(&batch_arc), Arc::clone(&row_type), 0, None)?;
        for i in 0..batch.num_rows() {
            row.set_row_id(i);
            let (bucket, key) = router.bucket_of(&row, num_buckets)?;
            groups
                .entry(bucket)
                .or_insert_with(|| (Vec::new(), key))
                .0
                .push(i as u32);
        }

        if groups.len() == 1 {
            let (_, (_, rep_key)) = groups.into_iter().next().unwrap();
            return self.send_arrow_batch(
                batch,
                physical_table_path,
                Some(rep_key),
                Some(num_buckets),
                deadline,
            );
        }

        let mut handles = Vec::with_capacity(groups.len());
        for (_bucket, (indices, rep_key)) in groups {
            let sub_batch = take_rows(&batch, &indices)?;
            let record = WriteRecord::for_append_record_batch(
                Arc::clone(&self.table_info),
                Arc::clone(&physical_table_path),
                self.table_info.schema_id,
                sub_batch,
            )
            .with_bucket_key(Some(rep_key))
            .with_submit_deadline(deadline);
            handles.push(
                self.writer_client
                    .send_with_bucket_count(&record, num_buckets)?,
            );
        }
        Ok(WriteResultFuture::join(handles))
    }

    fn send_arrow_batch(
        &self,
        batch: RecordBatch,
        physical_table_path: Arc<PhysicalTablePath>,
        bucket_key: Option<Bytes>,
        bucket_count: Option<i32>,
        deadline: Option<Instant>,
    ) -> Result<WriteResultFuture> {
        let record = WriteRecord::for_append_record_batch(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            batch,
        )
        .with_bucket_key(bucket_key)
        .with_submit_deadline(deadline);
        let result_handle = match bucket_count {
            Some(bucket_count) => self
                .writer_client
                .send_with_bucket_count(&record, bucket_count)?,
            None => self.writer_client.send(&record)?,
        };
        Ok(WriteResultFuture::new(result_handle))
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let idx = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &idx, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| UnexpectedError {
            message: format!("Failed to split arrow batch by bucket: {e}"),
            source: None,
        })?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| UnexpectedError {
        message: format!("Failed to rebuild split arrow batch: {e}"),
        source: None,
    })
}
