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

use crate::error::{ApiError, Error, Result};
use crate::record::LogRecordBatch;
use bytes::Bytes;
use crc32c::{crc32c, crc32c_append};

const STATE_MAGIC: &[u8; 4] = b"FLPB";
const STATE_VERSION: u8 = 1;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const CHECKSUM_OFFSET: usize = 4 + 1 + 8 + 8 + 4 + 8 + 4 + 4 + 2;
const HEADER_SIZE: usize = CHECKSUM_OFFSET + 4;

/// Explicit server operation for a prepared batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoverableWriteMode {
    /// First dispatch. Persist the caller's `sending` marker before invoking this operation.
    NewSend,
    /// Replay after any possibly transmitted attempt, including after process restart.
    Retry,
}

/// Single-bucket result returned by a prepared append attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedLogBatchResult {
    pub bucket_id: i32,
    pub base_offset: i64,
}

impl RecoverableWriteMode {
    pub(crate) fn as_protocol_value(self) -> i32 {
        match self {
            Self::NewSend => 1,
            Self::Retry => 2,
        }
    }
}

/// Immutable, caller-persistable identity and bytes for one prepared log append.
///
/// Persist [`Self::encode`] before dispatching the first send. After any attempt that may have
/// reached a server, replay the same state with retry mode; do not rebuild it from rows. This
/// value represents one bucket only. Callers must serialize ownership and allow at most one
/// uncertain batch per bucket. The checksum is the record format's CRC32C, not an authentication
/// mechanism. Restore state only through a client configured for the same Fluss cluster; the
/// state binds table id, schema id, partition, and bucket but does not contain a cluster UUID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedLogBatch {
    table_id: i64,
    partition_id: Option<i64>,
    bucket_id: i32,
    writer_id: i64,
    batch_sequence: i32,
    schema_id: i16,
    records: Bytes,
}

impl PreparedLogBatch {
    /// Creates state from one already encoded Fluss Arrow log record batch.
    pub fn new(
        table_id: i64,
        partition_id: Option<i64>,
        bucket_id: i32,
        writer_id: i64,
        batch_sequence: i32,
        records: Bytes,
    ) -> Result<Self> {
        if table_id < 0 || bucket_id < 0 || writer_id < 0 || batch_sequence < 0 {
            return Err(invalid(
                "table, bucket, writer, or sequence is out of range",
            ));
        }
        if records.is_empty() || records.len() > MAX_BATCH_BYTES {
            return Err(invalid("prepared batch is empty or exceeds 64 MiB"));
        }
        let inner_length = records
            .get(crate::record::LENGTH_OFFSET..crate::record::LENGTH_OFFSET + 4)
            .ok_or_else(|| invalid("prepared batch is shorter than its fixed header"))?;
        let inner_length = i32::from_le_bytes(inner_length.try_into().unwrap());
        let declared_size = usize::try_from(inner_length)
            .ok()
            .and_then(|length| length.checked_add(crate::record::LOG_OVERHEAD))
            .ok_or_else(|| invalid("prepared batch has an invalid signed length"))?;
        if declared_size != records.len() {
            return Err(invalid("prepared batch length does not match its bytes"));
        }
        let batch = LogRecordBatch::new(records.clone());
        if !batch.is_valid()
            || batch.size_in_bytes() != records.len()
            || batch.writer_id() != writer_id
            || batch.batch_sequence() != batch_sequence
        {
            return Err(invalid(
                "record bytes are corrupt or identity does not match",
            ));
        }
        Ok(Self {
            table_id,
            partition_id,
            bucket_id,
            writer_id,
            batch_sequence,
            schema_id: batch.schema_id(),
            records,
        })
    }

    /// Restores and validates a versioned state blob. No network request is made on failure.
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() < HEADER_SIZE || encoded.get(..4) != Some(STATE_MAGIC.as_slice()) {
            return Err(invalid("prepared state has an invalid header"));
        }
        if encoded[4] != STATE_VERSION {
            return Err(invalid("unsupported prepared-state version"));
        }
        let expected_checksum =
            u32::from_le_bytes(encoded[CHECKSUM_OFFSET..HEADER_SIZE].try_into().unwrap());
        let actual_checksum =
            crc32c_append(crc32c(&encoded[..CHECKSUM_OFFSET]), &encoded[HEADER_SIZE..]);
        if expected_checksum != actual_checksum {
            return Err(invalid("prepared-state checksum mismatch"));
        }
        let table_id = i64::from_le_bytes(encoded[5..13].try_into().unwrap());
        let raw_partition = i64::from_le_bytes(encoded[13..21].try_into().unwrap());
        let bucket_id = i32::from_le_bytes(encoded[21..25].try_into().unwrap());
        let writer_id = i64::from_le_bytes(encoded[25..33].try_into().unwrap());
        let batch_sequence = i32::from_le_bytes(encoded[33..37].try_into().unwrap());
        let length = u32::from_le_bytes(encoded[37..41].try_into().unwrap()) as usize;
        if length == 0
            || length > MAX_BATCH_BYTES
            || HEADER_SIZE.checked_add(length) != Some(encoded.len())
        {
            return Err(invalid("prepared-state batch length is invalid"));
        }
        let value = Self::new(
            table_id,
            (raw_partition != -1).then_some(raw_partition),
            bucket_id,
            writer_id,
            batch_sequence,
            Bytes::copy_from_slice(&encoded[HEADER_SIZE..]),
        )?;
        if value.schema_id != i16::from_le_bytes(encoded[41..43].try_into().unwrap()) {
            return Err(invalid("prepared-state schema id does not match batch"));
        }
        Ok(value)
    }

    /// Encodes this state for caller-managed durable storage.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + self.records.len());
        out.extend_from_slice(STATE_MAGIC);
        out.push(STATE_VERSION);
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(&self.partition_id.unwrap_or(-1).to_le_bytes());
        out.extend_from_slice(&self.bucket_id.to_le_bytes());
        out.extend_from_slice(&self.writer_id.to_le_bytes());
        out.extend_from_slice(&self.batch_sequence.to_le_bytes());
        out.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.schema_id.to_le_bytes());
        let checksum = crc32c_append(crc32c(&out), &self.records);
        out.extend_from_slice(&checksum.to_le_bytes());
        out.extend_from_slice(&self.records);
        out
    }

    pub(crate) fn result_from_response(
        response: crate::proto::ProduceLogResponse,
        expected_partition_id: Option<i64>,
        expected_bucket_id: i32,
    ) -> Result<PreparedLogBatchResult> {
        let [bucket] = response.buckets_resp.as_slice() else {
            return Err(unexpected(
                "prepared produce returned an invalid bucket count",
            ));
        };
        if bucket.bucket_id != expected_bucket_id || bucket.partition_id != expected_partition_id {
            return Err(unexpected(
                "prepared produce returned a different physical bucket",
            ));
        }
        if let Some(code) = bucket.error_code.filter(|code| *code != 0) {
            return Err(Error::FlussAPIError {
                api_error: ApiError {
                    code,
                    message: bucket
                        .error_message
                        .clone()
                        .unwrap_or_else(|| format!("ProduceLog failed with error code {code}")),
                },
            });
        }
        let base_offset = bucket
            .base_offset
            .ok_or_else(|| unexpected("successful prepared produce omitted base offset"))?;
        Ok(PreparedLogBatchResult {
            bucket_id: bucket.bucket_id,
            base_offset,
        })
    }

    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    pub fn partition_id(&self) -> Option<i64> {
        self.partition_id
    }

    pub fn bucket_id(&self) -> i32 {
        self.bucket_id
    }

    pub fn writer_id(&self) -> i64 {
        self.writer_id
    }

    pub fn batch_sequence(&self) -> i32 {
        self.batch_sequence
    }

    pub fn schema_id(&self) -> i16 {
        self.schema_id
    }

    pub fn records(&self) -> &Bytes {
        &self.records
    }
}

fn invalid(message: &str) -> Error {
    Error::IllegalArgument {
        message: message.to_string(),
    }
}

fn unexpected(message: &str) -> Error {
    Error::UnexpectedError {
        message: message.to_string(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::build_append_only_batch;

    #[test]
    fn rejects_truncated_and_unknown_state_versions() {
        assert!(PreparedLogBatch::decode(b"FLPB").is_err());
        let mut unknown = vec![0; HEADER_SIZE];
        unknown[..4].copy_from_slice(STATE_MAGIC);
        unknown[4] = STATE_VERSION + 1;
        assert!(PreparedLogBatch::decode(&unknown).is_err());
    }

    #[test]
    fn encoded_state_round_trips_exact_batch_bytes_and_identity() {
        let (_, mut bytes) = build_append_only_batch(&[(7, "durable")]);
        let writer_id = 42_i64;
        let sequence = 3_i32;
        // V0 log batch header offsets: writer id at 32, sequence at 40, schema starts at 25.
        bytes[32..40].copy_from_slice(&writer_id.to_le_bytes());
        bytes[40..44].copy_from_slice(&sequence.to_le_bytes());
        let checksum = crc32c(&bytes[25..]);
        bytes[21..25].copy_from_slice(&checksum.to_le_bytes());

        let prepared =
            PreparedLogBatch::new(123, Some(9), 2, writer_id, sequence, Bytes::from(bytes))
                .unwrap();
        let restored = PreparedLogBatch::decode(&prepared.encode()).unwrap();

        assert_eq!(restored, prepared);
        assert_eq!(restored.records(), prepared.records());
        let mut corrupt = prepared.encode();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(PreparedLogBatch::decode(&corrupt).is_err());
    }

    #[test]
    fn envelope_metadata_corruption_is_rejected() {
        let prepared = test_prepared();
        let encoded = prepared.encode();
        for offset in [5, 13, 21, 25, 33, 37, 41, HEADER_SIZE] {
            let mut corrupted = encoded.clone();
            corrupted[offset] ^= 1;
            assert!(
                PreparedLogBatch::decode(&corrupted).is_err(),
                "offset {offset}"
            );
        }
    }

    #[test]
    fn invalid_signed_inner_length_is_rejected_without_panicking() {
        let mut records = test_prepared().records().to_vec();
        records[8..12].copy_from_slice(&(-1_i32).to_le_bytes());
        let result = std::panic::catch_unwind(|| {
            PreparedLogBatch::new(123, Some(9), 2, 42, 3, Bytes::from(records))
        });
        assert!(result.is_ok(), "invalid inner length must not panic");
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn per_bucket_error_is_an_error_not_a_success_result() {
        let response = crate::proto::ProduceLogResponse {
            buckets_resp: vec![crate::proto::PbProduceLogRespForBucket {
                partition_id: Some(9),
                bucket_id: 2,
                error_code: Some(crate::rpc::FlussError::UnknownWriterIdException.code()),
                error_message: Some("writer expired".to_string()),
                base_offset: None,
                original_partition_name: None,
            }],
        };
        let error = PreparedLogBatch::result_from_response(response, Some(9), 2).unwrap_err();
        assert_eq!(
            error.api_error(),
            Some(crate::rpc::FlussError::UnknownWriterIdException)
        );
    }

    #[test]
    fn missing_or_mismatched_bucket_response_is_not_success() {
        let response = crate::proto::ProduceLogResponse::default();
        assert!(PreparedLogBatch::result_from_response(response, None, 2).is_err());
        let response = crate::proto::ProduceLogResponse {
            buckets_resp: vec![crate::proto::PbProduceLogRespForBucket {
                partition_id: None,
                bucket_id: 3,
                error_code: Some(0),
                error_message: None,
                base_offset: Some(12),
                original_partition_name: None,
            }],
        };
        assert!(PreparedLogBatch::result_from_response(response, None, 2).is_err());
    }

    fn test_prepared() -> PreparedLogBatch {
        let (_, mut bytes) = build_append_only_batch(&[(7, "durable")]);
        let writer_id = 42_i64;
        let sequence = 3_i32;
        bytes[32..40].copy_from_slice(&writer_id.to_le_bytes());
        bytes[40..44].copy_from_slice(&sequence.to_le_bytes());
        let checksum = crc32c(&bytes[25..]);
        bytes[21..25].copy_from_slice(&checksum.to_le_bytes());
        PreparedLogBatch::new(123, Some(9), 2, writer_id, sequence, Bytes::from(bytes)).unwrap()
    }
}
