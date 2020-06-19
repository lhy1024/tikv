// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use std::fmt::{self, Display, Formatter};

lazy_static! {
    pub static ref PD_REQUEST_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_pd_request_duration_seconds",
        "Bucketed histogram of PD requests duration",
        &["type"]
    )
    .unwrap();
    pub static ref PD_HEARTBEAT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_pd_heartbeat_message_total",
        "Total number of PD heartbeat messages.",
        &["type"]
    )
    .unwrap();
    pub static ref PD_VALIDATE_PEER_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_pd_validate_peer_total",
        "Total number of pd worker validate peer task.",
        &["type"]
    )
    .unwrap();
    pub static ref STORE_SIZE_GAUGE_VEC: IntGaugeVec =
        register_int_gauge_vec!("tikv_store_size_bytes", "Size of storage.", &["type"]).unwrap();
    pub static ref REGION_READ_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_read_keys",
        "Histogram of keys written for regions",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_READ_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_read_bytes",
        "Histogram of bytes written for regions",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_written_bytes",
        "Histogram of bytes written for regions",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref REGION_WRITTEN_KEYS_HISTOGRAM: Histogram = register_histogram!(
        "tikv_region_written_keys",
        "Histogram of keys written for regions",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

#[derive(Clone)]
pub enum GrpcTypeKind {
    Coprocessor,
    KvGet,
    KvBatchGet,
    KvBatchGetCommand,
    RawGet,
    RawBatchGetCommand,
    RawBatchGet,
    RawScan,
    RawBatchScan,
    KvScan,
    RawPut,
    RawBatchPut,
    KvPreWrite,
    KvCommit,
}

impl Display for GrpcTypeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            GrpcTypeKind::Coprocessor => write!(f, "Coprocessor"),
            GrpcTypeKind::KvGet => write!(f, "KvGet"),
            GrpcTypeKind::KvBatchGet => write!(f, "KvBatchGet"),
            GrpcTypeKind::KvBatchGetCommand => write!(f, "KvBatchGetCommand"),
            GrpcTypeKind::RawGet => write!(f, "RawGet"),
            GrpcTypeKind::RawBatchGetCommand => write!(f, "RawBatchGetCommand"),
            GrpcTypeKind::RawBatchGet => write!(f, "RawBatchGet"),
            GrpcTypeKind::RawScan => write!(f, "RawScan"),
            GrpcTypeKind::RawBatchScan => write!(f, "RawBatchScan"),
            GrpcTypeKind::KvScan => write!(f, "KvScan"),
            GrpcTypeKind::RawPut => write!(f, "RawPut"),
            GrpcTypeKind::RawBatchPut => write!(f, "RawBatchPut"),
            GrpcTypeKind::KvPreWrite => write!(f, "KvPreWrite"),
            GrpcTypeKind::KvCommit => write!(f, "KvCommit"),
        }
    }
}
