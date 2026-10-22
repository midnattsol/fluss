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

#[cfg(test)]
mod prepared_log_batch_test {
    use crate::integration::utils::{create_table, get_shared_cluster, wait_for_table_ready};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use fluss::client::EARLIEST_OFFSET;
    use fluss::client::{PreparedLogBatch, RecoverableWriteMode};
    use fluss::error::FlussError;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::row::DataGetters;
    use fluss::rpc::message::OffsetSpec;
    use std::sync::Arc;

    const SOURCE_SERVER_REQUIRED: &str = "requires a current-source Fluss image; see the Rust README recoverable-write integration instructions";

    #[tokio::test]
    async fn prepared_batch_restore_retry_and_unknown_writer_error_against_server() {
        let cluster = get_shared_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "prepared_log_batch_recovery");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("payload", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .expect("table descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        let batch = arrow_batch(10, "persist and replay");
        let prepared = writer
            .prepare_log_batch(batch.clone(), 0, 7001, 0)
            .await
            .expect("prepare batch");
        let caller_checkpoint = prepared.encode();

        // Drop the first response to model an uncertain caller outcome, then restore only the
        // caller-persisted state and issue RETRY against the live Fluss server.
        let _uncertain_first_response = writer
            .send_prepared_log_batch(&prepared, RecoverableWriteMode::NewSend)
            .await
            .unwrap_or_else(|error| {
                panic!("{SOURCE_SERVER_REQUIRED}; initial prepared send failed: {error}")
            });
        let restored = PreparedLogBatch::decode(&caller_checkpoint).expect("restore state");
        let replay = writer
            .send_prepared_log_batch(&restored, RecoverableWriteMode::Retry)
            .await
            .expect("replay exact prepared bytes");
        assert_eq!(replay.bucket_id, 0);
        assert_eq!(replay.base_offset, 0);

        let offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("latest offset");
        assert_eq!(offsets.get(&0), Some(&1));

        let unknown_writer = writer
            .prepare_log_batch(batch, 0, 7002, 0)
            .await
            .expect("prepare unknown writer batch");
        let error = writer
            .send_prepared_log_batch(&unknown_writer, RecoverableWriteMode::Retry)
            .await
            .expect_err("unknown retry must not look acknowledged");
        assert_eq!(
            error.api_error(),
            Some(FlussError::UnknownWriterIdException)
        );
        let offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("offset after unknown writer retry");
        assert_eq!(offsets.get(&0), Some(&1));

        let scanner = table.new_scan().create_log_scanner().expect("scanner");
        scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("subscribe");
        let records = scanner
            .poll(std::time::Duration::from_secs(5))
            .await
            .expect("scan persisted row");
        assert_eq!(records.count(), 1);
        let record = records
            .records_by_buckets()
            .values()
            .flatten()
            .next()
            .expect("one persisted record");
        assert_eq!(record.row().get_int(0).unwrap(), 10);
    }

    #[tokio::test]
    async fn dropped_produce_ack_is_replayed_without_duplicate_append() {
        use fluss::error::Error;
        use fluss_test_cluster::FlussTestingClusterBuilder;
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let proxy_port = proxy_listener.local_addr().unwrap().port();
        let coordinator_port = available_port_pair().await;
        let tablet_port = coordinator_port + 1;
        let (dropped_ack_tx, dropped_ack_rx) = oneshot::channel();
        let proxy = tokio::spawn(run_drop_one_produce_ack_proxy(
            proxy_listener,
            format!("127.0.0.1:{tablet_port}"),
            dropped_ack_tx,
        ));

        let cluster_suffix = uuid::Uuid::new_v4().simple().to_string();
        let mut builder =
            FlussTestingClusterBuilder::new(format!("rust-ackloss-{}", &cluster_suffix[..8]))
                .with_port(coordinator_port)
                .with_tablet_advertised_client_port(proxy_port);
        let cluster = builder.build().await;
        wait_for_tablet_server_ready(&cluster).await;
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().expect("admin");
        let table_path = TablePath::new("fluss", "prepared_log_batch_ack_loss");
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("payload", DataTypes::string())
                    .build()
                    .expect("schema"),
            )
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .expect("table descriptor");
        create_table(&admin, &table_path, &descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection.get_table(&table_path).await.expect("table");
        let writer = table
            .new_append()
            .expect("append")
            .create_writer()
            .expect("writer");
        let prepared = writer
            .prepare_log_batch(arrow_batch(31, "ack lost after commit"), 0, 7101, 0)
            .await
            .expect("prepare batch");
        let durable_caller_state = prepared.encode();

        let first_error = writer
            .send_prepared_log_batch(&prepared, RecoverableWriteMode::NewSend)
            .await
            .expect_err("proxy must drop the ProduceLog acknowledgment");
        assert!(
            !matches!(first_error, Error::UnsupportedVersion { .. }),
            "{SOURCE_SERVER_REQUIRED}; got {first_error}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped_ack_rx)
            .await
            .expect("proxy did not observe the committed ProduceLog response")
            .expect("proxy acknowledgment signal");

        let offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("latest offset after lost acknowledgment");
        assert_eq!(offsets.get(&0), Some(&1));

        let restored = PreparedLogBatch::decode(&durable_caller_state).expect("restore state");
        let replay = writer
            .send_prepared_log_batch(&restored, RecoverableWriteMode::Retry)
            .await
            .expect("retry exact persisted bytes");
        assert_eq!(replay.base_offset, 0);
        let offsets = admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .expect("latest offset after replay");
        assert_eq!(offsets.get(&0), Some(&1));

        proxy.abort();
        drop(cluster);
    }

    async fn available_port_pair() -> u16 {
        loop {
            let coordinator = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = coordinator.local_addr().unwrap().port();
            if port == u16::MAX {
                continue;
            }
            if tokio::net::TcpListener::bind(("127.0.0.1", port + 1))
                .await
                .is_ok()
            {
                return port;
            }
        }
    }

    async fn wait_for_tablet_server_ready(cluster: &fluss_test_cluster::FlussTestingCluster) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let connection = cluster.get_fluss_connection().await;
                if connection
                    .get_metadata()
                    .get_cluster()
                    .get_one_available_server()
                    .is_some()
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        })
        .await
        .expect("tablet server did not register with the coordinator");
    }

    async fn run_drop_one_produce_ack_proxy(
        listener: tokio::net::TcpListener,
        upstream: String,
        dropped_ack: tokio::sync::oneshot::Sender<()>,
    ) {
        use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let drop_next_ack = Arc::new(AtomicBool::new(true));
        let pending_produce_id = Arc::new(AtomicI32::new(-1));
        let dropped_ack = Arc::new(Mutex::new(Some(dropped_ack)));
        loop {
            let (client, _) = listener.accept().await.unwrap();
            let server = TcpStream::connect(&upstream).await.unwrap();
            let (client_read, mut client_write) = client.into_split();
            let (mut server_read, server_write) = server.into_split();

            let mut request_forwarder = {
                let pending_id = Arc::clone(&pending_produce_id);
                tokio::spawn(async move {
                    let mut client_read = client_read;
                    let mut server_write = server_write;
                    loop {
                        let length = match client_read.read_i32().await {
                            Ok(length) => length,
                            Err(_) => return,
                        };
                        let mut frame = vec![0; usize::try_from(length).unwrap()];
                        if client_read.read_exact(&mut frame).await.is_err() {
                            return;
                        }
                        if frame.len() >= 8 && i16::from_be_bytes([frame[0], frame[1]]) == 1014 {
                            pending_id.store(
                                i32::from_be_bytes(frame[4..8].try_into().unwrap()),
                                Ordering::SeqCst,
                            );
                        }
                        if server_write.write_i32(length).await.is_err()
                            || server_write.write_all(&frame).await.is_err()
                            || server_write.flush().await.is_err()
                        {
                            return;
                        }
                    }
                })
            };

            let response_forwarder = {
                let drop_next_ack = Arc::clone(&drop_next_ack);
                let pending_id = Arc::clone(&pending_produce_id);
                let dropped_ack = Arc::clone(&dropped_ack);
                async move {
                    loop {
                        let length = match server_read.read_i32().await {
                            Ok(length) => length,
                            Err(_) => return,
                        };
                        let mut frame = vec![0; usize::try_from(length).unwrap()];
                        if server_read.read_exact(&mut frame).await.is_err() {
                            return;
                        }
                        if frame.len() >= 5 {
                            let response_id = i32::from_be_bytes(frame[1..5].try_into().unwrap());
                            if pending_id.load(Ordering::SeqCst) == response_id
                                && drop_next_ack.swap(false, Ordering::SeqCst)
                            {
                                if let Some(signal) = dropped_ack.lock().unwrap().take() {
                                    let _ = signal.send(());
                                }
                                let _ = client_write.shutdown().await;
                                return;
                            }
                        }
                        if client_write.write_i32(length).await.is_err()
                            || client_write.write_all(&frame).await.is_err()
                            || client_write.flush().await.is_err()
                        {
                            return;
                        }
                    }
                }
            };

            tokio::select! {
                _ = &mut request_forwarder => {},
                _ = response_forwarder => request_forwarder.abort(),
            }
        }
    }

    fn arrow_batch(id: i32, value: &str) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .expect("Arrow batch")
    }
}
