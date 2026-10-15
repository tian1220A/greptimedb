// Copyright 2026 Greptime Team
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

//! End-to-end verification for `fetch_columns` against a live MySQL/PostgreSQL
//! instance. Ignored by default — run with the target database reachable:
//!
//! ```sh
//! MYSQL_DSN=mysql://root:greptime@127.0.0.1:3306/testdb \
//! POSTGRES_DSN=postgres://postgres:greptime@127.0.0.1:5432/testdb \
//! cargo test -p relational-engine --test fetch_columns -- --ignored --nocapture
//! ```

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use relational_engine::source::{fetch_columns, ConnectionInfo, RelationalColumn};

fn names(cols: &[RelationalColumn]) -> Vec<&str> {
    cols.iter().map(|c| c.name.as_str()).collect()
}

#[tokio::test]
#[ignore]
async fn test_fetch_columns_mysql() {
    let dsn = std::env::var("MYSQL_DSN").unwrap_or_else(|_| {
        "mysql://root:greptime@127.0.0.1:3306/testdb".to_string()
    });
    let connection = ConnectionInfo::parse(&dsn).unwrap();
    let cols = fetch_columns(&connection, "sample").await.unwrap();

    assert_eq!(
        names(&cols),
        vec![
            "id", "big_id", "name", "price", "ratio", "created_at", "birthday", "note", "data",
            "tags", "active",
        ]
    );
    assert_eq!(cols[0].data_type, ConcreteDataType::int32_datatype());
    // BIGINT UNSIGNED maps to UInt64 (fits 2^64-1 without overflow).
    assert_eq!(cols[1].data_type, ConcreteDataType::uint64_datatype());
    assert_eq!(cols[2].data_type, ConcreteDataType::string_datatype());
    assert_eq!(
        cols[3].data_type,
        ConcreteDataType::decimal128_datatype(12, 2)
    );
    assert_eq!(cols[4].data_type, ConcreteDataType::float64_datatype());
    assert_eq!(
        cols[5].data_type,
        ConcreteDataType::timestamp_millisecond_datatype()
    );
    assert_eq!(cols[6].data_type, ConcreteDataType::date_datatype());
    assert_eq!(cols[7].data_type, ConcreteDataType::string_datatype());
    assert_eq!(cols[8].data_type, ConcreteDataType::binary_datatype());
    assert_eq!(cols[9].data_type, ConcreteDataType::json_datatype());
    // MySQL BOOLEAN is indistinguishable from TINYINT(1).
    assert_eq!(cols[10].data_type, ConcreteDataType::int8_datatype());
}

#[tokio::test]
#[ignore]
async fn test_fetch_columns_postgres() {
    let dsn = std::env::var("POSTGRES_DSN").unwrap_or_else(|_| {
        "postgres://postgres:greptime@127.0.0.1:5432/testdb".to_string()
    });
    let connection = ConnectionInfo::parse(&dsn).unwrap();
    let cols = fetch_columns(&connection, "sample").await.unwrap();

    assert_eq!(
        names(&cols),
        vec![
            "id", "big_id", "name", "price", "ratio", "created_at", "birthday", "note", "data",
            "tags", "active", "nums",
        ]
    );
    assert_eq!(cols[0].data_type, ConcreteDataType::int32_datatype());
    assert_eq!(cols[1].data_type, ConcreteDataType::int64_datatype());
    assert_eq!(cols[2].data_type, ConcreteDataType::string_datatype());
    assert_eq!(
        cols[3].data_type,
        ConcreteDataType::decimal128_datatype(12, 2)
    );
    assert_eq!(cols[4].data_type, ConcreteDataType::float64_datatype());
    assert_eq!(
        cols[5].data_type,
        ConcreteDataType::timestamp_millisecond_datatype()
    );
    assert_eq!(cols[6].data_type, ConcreteDataType::date_datatype());
    assert_eq!(cols[7].data_type, ConcreteDataType::string_datatype());
    assert_eq!(cols[8].data_type, ConcreteDataType::binary_datatype());
    assert_eq!(cols[9].data_type, ConcreteDataType::json_datatype());
    assert_eq!(cols[10].data_type, ConcreteDataType::boolean_datatype());
    // PostgreSQL arrays map to List<element>.
    assert_eq!(
        cols[11].data_type,
        ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype()))
    );
}
