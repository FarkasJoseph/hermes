use crate::types::common::deserialize_optional_string_or_number;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// A YAMCS bucket, used for storing arbitrary objects (e.g. downlinked files)
///
/// YAMCS renders 64-bit integers (`size`, `maxSize`) as JSON strings, per proto3 JSON's
/// convention for int64/uint64 - confirmed against a live server (`GET /api/buckets/{instance}`
/// returns e.g. `"size": "4174776"`), not the bare number these fields' Rust types would
/// otherwise expect.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub name: String,
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    pub size: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    pub max_size: Option<u64>,
    pub num_objects: Option<u32>,
    /// Note: YAMCS's field is `maxObjects`, not `maxNumObjects` (confirmed against a live
    /// server); `rename` overrides the struct's `rename_all = "camelCase"` default.
    #[serde(rename = "maxObjects")]
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
///
/// See `Bucket` above: `size` is serialized as a JSON string by YAMCS, not a number.
#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketObject {
    pub name: String,
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Real response body from `GET /api/buckets/{instance}/{bucket}/objects` against a live
    /// YAMCS 5.12.0 server. `size` arrives as a JSON string, which is what this test guards.
    #[test]
    fn list_objects_response_deserializes_string_sizes() {
        let json = r#"{
            "objects": [{
                "name": "./DpCat/Dp_268521472_1788986684_00693201.fdp",
                "created": "2026-09-09T22:54:58.897Z",
                "size": "1277",
                "contentType": "application/octet-stream"
            }, {
                "name": "pic.bin",
                "created": "2026-09-08T22:05:08.482Z",
                "size": "2048000",
                "contentType": "application/octet-stream"
            }]
        }"#;
        let response: ListObjectsResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(response.objects.len(), 2);
        assert_eq!(response.objects[0].size, Some(1277));
        assert_eq!(response.objects[1].size, Some(2048000));
    }

    /// Real response body from `GET /api/buckets/{instance}`.
    #[test]
    fn list_buckets_response_deserializes_string_sizes() {
        let json = r#"{
            "buckets": [{
                "name": "fprimeFilesIn",
                "size": "4174776",
                "numObjects": 5,
                "maxSize": "104857600",
                "maxObjects": 1000
            }]
        }"#;
        let response: ListBucketsResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(response.buckets.len(), 1);
        assert_eq!(response.buckets[0].size, Some(4174776));
        assert_eq!(response.buckets[0].max_size, Some(104857600));
        assert_eq!(response.buckets[0].num_objects, Some(5));
    }
}
