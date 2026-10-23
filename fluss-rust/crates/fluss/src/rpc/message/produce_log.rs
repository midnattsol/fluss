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

use crate::error::Result as FlussResult;
use crate::proto::{PbProduceLogReqForBucket, ProduceLogResponse};
use crate::rpc::frame::ReadError;

use crate::client::ReadyWriteBatch;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{TableId, impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

pub struct ProduceLogRequest {
    pub(crate) inner_request: proto::ProduceLogRequest,
}

impl ProduceLogRequest {
    pub fn new(
        table_id: TableId,
        ack: i16,
        max_request_timeout_ms: i32,
        ready_batches: &mut [ReadyWriteBatch],
    ) -> FlussResult<Self> {
        let mut request = proto::ProduceLogRequest {
            table_id,
            acks: ack as i32,
            timeout_ms: max_request_timeout_ms,
            ..Default::default()
        };
        for ready_batch in ready_batches {
            request.buckets_req.push(PbProduceLogReqForBucket {
                partition_id: ready_batch.table_bucket.partition_id(),
                bucket_id: ready_batch.table_bucket.bucket_id(),
                records: ready_batch.write_batch.build()?,
                original_partition_name: None,
                routing_bucket_count: ready_batch.write_batch.routing_bucket_count(),
            })
        }

        Ok(ProduceLogRequest {
            inner_request: request,
        })
    }

    pub(crate) fn new_prepared(
        table_id: TableId,
        partition_id: Option<i64>,
        bucket_id: i32,
        records: bytes::Bytes,
        recovery_mode: i32,
    ) -> Self {
        ProduceLogRequest {
            inner_request: proto::ProduceLogRequest {
                table_id,
                acks: -1,
                timeout_ms: 30_000,
                buckets_req: vec![PbProduceLogReqForBucket {
                    partition_id,
                    bucket_id,
                    records,
                    original_partition_name: None,
                    routing_bucket_count: None,
                }],
                recovery_mode: Some(recovery_mode),
            },
        }
    }
}

impl RequestBody for ProduceLogRequest {
    type ResponseBody = ProduceLogResponse;

    const API_KEY: ApiKey = ApiKey::ProduceLog;

    fn check_version(
        &self,
        version: crate::rpc::api_version::ApiVersion,
    ) -> Result<(), crate::error::Error> {
        if self.inner_request.recovery_mode.is_some() && version.0 < 2 {
            return Err(crate::error::Error::UnsupportedVersion {
                message: format!(
                    "recoverable PRODUCE_LOG requires version 2, negotiated version {}",
                    version.0
                ),
            });
        }
        Ok(())
    }
}

impl_write_type!(ProduceLogRequest);
impl_read_type!(ProduceLogResponse);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::api_key::ApiKey;
    use crate::rpc::api_version::{ApiVersion, ApiVersionRange};
    use crate::rpc::message::RequestBody;

    #[test]
    fn prepared_request_carries_mode_and_exact_records() {
        let bytes = bytes::Bytes::from_static(b"prepared-wire-bytes");
        let request = ProduceLogRequest::new_prepared(12, Some(8), 3, bytes.clone(), 2);

        assert_eq!(request.inner_request.recovery_mode, Some(2));
        assert_eq!(request.inner_request.buckets_req[0].records, bytes);
        assert_eq!(request.inner_request.buckets_req[0].partition_id, Some(8));
        assert_eq!(request.inner_request.buckets_req[0].bucket_id, 3);
        assert_eq!(ProduceLogRequest::API_KEY, ApiKey::ProduceLog);
        assert_eq!(
            ApiKey::ProduceLog.supported_versions(),
            Some(ApiVersionRange::new(ApiVersion(0), ApiVersion(2)))
        );
        assert!(request.check_version(ApiVersion(1)).is_err());
        assert!(request.check_version(ApiVersion(2)).is_ok());
    }
}
