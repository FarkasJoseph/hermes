use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// A YAMCS bucket, used for storing arbitrary objects (e.g. downlinked files)
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub name: String,
    pub size: Option<u64>,
    pub max_size: Option<u64>,
    pub num_objects: Option<u32>,
    pub max_num_objects: Option<u32>,
}

/// Response wrapper for listing buckets
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponse {
    #[serde(default)]
    pub buckets: Vec<Bucket>,
}

/// A single object stored in a bucket
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketObject {
    pub name: String,
    pub size: Option<u64>,
    /// Creation time, ISO 8601
    pub created: Option<String>,
    pub content_type: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

/// Response wrapper for listing objects within a bucket
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListObjectsResponse {
    #[serde(default)]
    pub objects: Vec<BucketObject>,
}
