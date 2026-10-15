// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_recordbatch::SendableRecordBatchStream;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::data_source::DataSourceRef;
use store_api::metadata::RegionMetadataRef;
use store_api::path_utils::region_name;
use store_api::region_request::{RegionCreateRequest, RegionOpenRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::requests::{RELATIONAL_CONNECTION_KEY, RELATIONAL_REMOTE_TABLE_KEY};

use crate::error::{MissingRequiredFieldSnafu, QueryRemoteSnafu, Result, UnsupportedSnafu};
use crate::manifest::RelationalRegionManifest;
use crate::source::{ConnectionInfo, MySqlSource, SqlDialectKind};

pub struct RelationalRegion {
    pub(crate) region_dir: String,
    pub(crate) metadata: RegionMetadataRef,
    data_source: DataSourceRef,
}

impl std::fmt::Debug for RelationalRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationalRegion")
            .field("region_dir", &self.region_dir)
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

pub type RelationalRegionRef = Arc<RelationalRegion>;

impl RelationalRegion {
    pub async fn create(
        region_id: RegionId,
        request: RegionCreateRequest,
        object_store: &ObjectStore,
    ) -> Result<RelationalRegionRef> {
        let manifest = RelationalRegionManifest {
            region_id,
            column_metadatas: request.column_metadatas.clone(),
            primary_key: request.primary_key.clone(),
            options: request.options,
        };

        let region_dir = object_store::util::join_dir(
            &request.table_dir,
            &region_name(region_id.table_id(), region_id.region_sequence()),
        );
        let metadata = manifest.metadata()?;
        let data_source = build_data_source(&metadata, &manifest.options)?;

        manifest.store(&region_dir, object_store).await?;

        Ok(Arc::new(Self {
            region_dir,
            metadata,
            data_source,
        }))
    }

    pub async fn open(
        region_id: RegionId,
        request: RegionOpenRequest,
        object_store: &ObjectStore,
    ) -> Result<RelationalRegionRef> {
        let region_dir = object_store::util::join_dir(
            &request.table_dir,
            &region_name(region_id.table_id(), region_id.region_sequence()),
        );
        let manifest = RelationalRegionManifest::load(region_id, &region_dir, object_store).await?;
        let metadata = manifest.metadata()?;
        let data_source = build_data_source(&metadata, &manifest.options)?;

        Ok(Arc::new(Self {
            region_dir,
            metadata,
            data_source,
        }))
    }

    pub async fn drop(&self, object_store: &ObjectStore) -> Result<()> {
        RelationalRegionManifest::delete(self.metadata.region_id, &self.region_dir, object_store)
            .await
    }

    pub fn metadata(&self) -> RegionMetadataRef {
        self.metadata.clone()
    }

    /// Scans the remote table, returning a stream of record batches. The
    /// underlying data source defers the actual connection + streaming work to a
    /// spawned task, so this is synchronous.
    pub fn query(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        self.data_source.get_stream(request).context(QueryRemoteSnafu)
    }
}

/// Builds the data source for a region from its frozen manifest options.
fn build_data_source(
    metadata: &RegionMetadataRef,
    options: &HashMap<String, String>,
) -> Result<DataSourceRef> {
    let connection = options.get(RELATIONAL_CONNECTION_KEY).ok_or_else(|| {
        MissingRequiredFieldSnafu {
            name: RELATIONAL_CONNECTION_KEY,
        }
        .build()
    })?;
    let remote_table = options
        .get(RELATIONAL_REMOTE_TABLE_KEY)
        .ok_or_else(|| {
            MissingRequiredFieldSnafu {
                name: RELATIONAL_REMOTE_TABLE_KEY,
            }
            .build()
        })?;

    let connection = ConnectionInfo::parse(connection)?;

    match connection.kind {
        SqlDialectKind::MySql => {
            let time_index_pos = metadata.time_index_column_pos();
            // The frozen schema is the remote columns followed by the synthetic
            // `greptime_timestamp` column; drop the latter from the remote list.
            let mut remote_columns = Vec::with_capacity(metadata.column_metadatas.len() - 1);
            for (index, column) in metadata.column_metadatas.iter().enumerate() {
                if index == time_index_pos {
                    continue;
                }
                remote_columns.push(column.column_schema.name.clone());
            }
            let source = MySqlSource::new(
                connection,
                remote_table.clone(),
                metadata.schema.clone(),
                remote_columns,
                time_index_pos,
            );
            Ok(Arc::new(source))
        }
        SqlDialectKind::Postgres => UnsupportedSnafu {
            operation: "PostgreSQL data source (planned in a later commit)",
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::collections::HashMap;

    use store_api::region_request::PathType;

    use super::*;
    use crate::error::Error;
    use crate::test_util::{new_test_column_metadata, new_test_object_store, new_test_options};

    #[tokio::test]
    async fn test_create_region() {
        let (_dir, object_store) = new_test_object_store("test_create_region");

        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: "create_region_dir/".to_string(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        let region = RelationalRegion::create(region_id, request.clone(), &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "create_region_dir/1_0000000000/");
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);
        assert_eq!(region.metadata.column_metadatas.len(), 3);

        assert!(
            object_store
                .exists("create_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        // Creating the same region twice must fail: the manifest already exists.
        let err = RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ManifestExists { .. });
    }

    #[tokio::test]
    async fn test_open_region() {
        let (_dir, object_store) = new_test_object_store("test_open_region");

        let region_dir = "open_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: region_dir.clone(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap();

        let request = RegionOpenRequest {
            engine: "relational".to_string(),
            table_dir: region_dir,
            path_type: PathType::Bare,
            options: HashMap::default(),
            skip_wal_replay: false,
            checkpoint: None,
            requirements: Default::default(),
        };

        let region = RelationalRegion::open(region_id, request, &object_store)
            .await
            .unwrap();

        assert_eq!(region.region_dir, "open_region_dir/1_0000000000/");
        assert_eq!(region.metadata.region_id, region_id);
        assert_eq!(region.metadata.primary_key, vec![1]);
        assert_eq!(region.metadata.column_metadatas.len(), 3);
    }

    #[tokio::test]
    async fn test_drop_region() {
        let (_dir, object_store) = new_test_object_store("test_drop_region");

        let region_dir = "drop_region_dir/".to_string();
        let request = RegionCreateRequest {
            engine: "relational".to_string(),
            column_metadatas: new_test_column_metadata(),
            primary_key: vec![1],
            options: new_test_options(),
            table_dir: region_dir.clone(),
            path_type: PathType::Bare,
            partition_expr_json: Some("".to_string()),
            requirements: Default::default(),
        };
        let region_id = RegionId::new(1, 0);

        let region = RelationalRegion::create(region_id, request, &object_store)
            .await
            .unwrap();

        assert!(
            object_store
                .exists("drop_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        RelationalRegion::drop(&region, &object_store)
            .await
            .unwrap();

        assert!(
            !object_store
                .exists("drop_region_dir/1_0000000000/manifest/_relational_manifest")
                .await
                .unwrap()
        );

        // Re-opening a dropped region must fail.
        let request = RegionOpenRequest {
            engine: "relational".to_string(),
            table_dir: region_dir,
            path_type: PathType::Bare,
            options: HashMap::default(),
            skip_wal_replay: false,
            checkpoint: None,
            requirements: Default::default(),
        };
        let err = RelationalRegion::open(region_id, request, &object_store)
            .await
            .unwrap_err();
        assert_matches!(err, Error::LoadRegionManifest { .. });
    }
}
