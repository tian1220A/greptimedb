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

//! PostgreSQL → GreptimeDB type mapping for the `infer_schema` step of `CREATE
//! EXTERNAL TABLE`.
//!
//! This is the forward direction (remote type → [`ConcreteDataType`]). It
//! inherits from GreptimeDB's own PostgreSQL server mapping
//! (`servers/src/postgres/types.rs::type_pg_to_gt`) and fixes two of its
//! simplifications: `numeric` maps to `Decimal128(p, s)` (not `UInt64`), and
//! `time` maps to `Time` (not `Timestamp`).
//!
//! Mapping decisions (see `类型映射核对表-PostgreSQL.md`):
//! - `numeric` with no precision (arbitrary precision) or `p > 38` is rejected.
//! - `timestamptz` timezone semantics are dropped (naive `Timestamp`, aligned
//!   with GreptimeDB's own PostgreSQL server).
//! - `interval` maps to `IntervalMonthDayNano` (months + days + seconds).
//! - Arrays (`_T`) map to `List<map(T)>`; the element must itself be mappable.
//! - `uuid` degrades to `String`; `jsonb` degrades to `Json`.
//! - `money`, `timetz`, `inet`/`cidr`/`macaddr`, `xml`, `bit`/`varbit`,
//!   `tsvector`/`tsquery`, ranges, composites and user-defined types are
//!   rejected.

use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use snafu::{ResultExt, ensure};

use crate::error::{
    ConnectRemoteSnafu, FetchMetadataSnafu, RemoteTableNotFoundSnafu, Result, UnsupportedSnafu,
};
use crate::source::connection::{box_foreign_error, split_remote_table, ConnectionInfo, RelationalColumn};

/// A PostgreSQL column type as reported by `INFORMATION_SCHEMA.COLUMNS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresColumnType {
    /// `udt_name` (the `pg_type` name): `int4`, `numeric`, `bpchar`, `_int4`, …
    pub type_name: String,
    /// `numeric_precision` for `numeric` / `decimal`.
    pub precision: Option<u64>,
    /// `numeric_scale` for `numeric` / `decimal`.
    pub scale: Option<i64>,
    /// `datetime_precision` (0..6) for `time` / `timestamp` / `timestamptz`.
    pub fsp: Option<u8>,
}

impl PostgresColumnType {
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            precision: None,
            scale: None,
            fsp: None,
        }
    }

    pub fn with_decimal(mut self, precision: u64, scale: i64) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    pub fn with_fsp(mut self, fsp: u8) -> Self {
        self.fsp = Some(fsp);
        self
    }
}

/// Maps a PostgreSQL column type to a GreptimeDB [`ConcreteDataType`].
pub fn map_type(col: &PostgresColumnType) -> Result<ConcreteDataType> {
    // An array's `pg_type` name is the element name prefixed with `_`. Array
    // dimensionality is a value-level property in PostgreSQL (both `int[]` and
    // `int[][]` share type `_int4`), so it is not visible here.
    if let Some(element) = col.type_name.strip_prefix('_') {
        let element_type = PostgresColumnType {
            type_name: element.to_string(),
            precision: col.precision,
            scale: col.scale,
            fsp: col.fsp,
        };
        let element = map_type(&element_type)?;
        return Ok(ConcreteDataType::list_datatype(Arc::new(element)));
    }

    match col.type_name.as_str() {
        "bool" => Ok(ConcreteDataType::boolean_datatype()),
        "int2" => Ok(ConcreteDataType::int16_datatype()),
        "int4" => Ok(ConcreteDataType::int32_datatype()),
        "int8" => Ok(ConcreteDataType::int64_datatype()),
        "float4" => Ok(ConcreteDataType::float32_datatype()),
        "float8" => Ok(ConcreteDataType::float64_datatype()),
        "numeric" | "decimal" => map_numeric(col.precision, col.scale),
        "text" | "varchar" | "bpchar" | "name" => Ok(ConcreteDataType::string_datatype()),
        "bytea" => Ok(ConcreteDataType::binary_datatype()),
        "date" => Ok(ConcreteDataType::date_datatype()),
        "time" => Ok(map_time(col.fsp)),
        "timetz" => unsupported("`time with time zone`"),
        "timestamp" | "timestamptz" => Ok(map_timestamp(col.fsp)),
        "interval" => Ok(ConcreteDataType::interval_month_day_nano_datatype()),
        "json" | "jsonb" => Ok(ConcreteDataType::json_datatype()),
        "uuid" => Ok(ConcreteDataType::string_datatype()),
        other => unsupported(format!("`{other}`")),
    }
}

fn map_numeric(precision: Option<u64>, scale: Option<i64>) -> Result<ConcreteDataType> {
    // A bare `numeric` (no precision) has arbitrary precision and cannot be
    // represented losslessly, so it is rejected.
    let Some(precision) = precision else {
        return unsupported("`numeric` without an explicit precision (arbitrary precision)");
    };
    let scale = scale.unwrap_or(0);
    if precision > 38 {
        return unsupported(format!("`numeric({precision}, {scale})` (precision > 38)"));
    }
    if scale < 0 || scale as u64 > precision {
        return unsupported(format!("`numeric({precision}, {scale})` (invalid scale)"));
    }
    Ok(ConcreteDataType::decimal128_datatype(
        precision as u8,
        scale as i8,
    ))
}

fn map_time(fsp: Option<u8>) -> ConcreteDataType {
    // PostgreSQL defaults to the maximum precision (6) for time/timestamp.
    match fsp.unwrap_or(6) {
        0 => ConcreteDataType::time_second_datatype(),
        1..=3 => ConcreteDataType::time_millisecond_datatype(),
        _ => ConcreteDataType::time_microsecond_datatype(),
    }
}

fn map_timestamp(fsp: Option<u8>) -> ConcreteDataType {
    match fsp.unwrap_or(6) {
        0 => ConcreteDataType::timestamp_second_datatype(),
        1..=3 => ConcreteDataType::timestamp_millisecond_datatype(),
        _ => ConcreteDataType::timestamp_microsecond_datatype(),
    }
}

fn unsupported(what: impl std::fmt::Display) -> Result<ConcreteDataType> {
    UnsupportedSnafu {
        operation: format!("map PostgreSQL type {what}"),
    }
    .fail()
}

/// Connects to the remote PostgreSQL instance and reads the column list of
/// `remote_table` from `information_schema.columns`, mapping each column to a
/// GreptimeDB [`ConcreteDataType`] via [`map_type`].
pub async fn fetch_columns(
    connection: &ConnectionInfo,
    remote_table: &str,
) -> Result<Vec<RelationalColumn>> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(connection.host.as_str())
        .port(connection.port)
        .user(connection.user.as_str())
        .password(connection.password.as_str())
        .dbname(connection.database.as_str());

    let (client, conn) = config.connect(tokio_postgres::NoTls)
        .await
        .map_err(|e| box_foreign_error(e))
        .context(ConnectRemoteSnafu)?;
    // Drive the connection to completion in the background so the client keeps
    // receiving responses.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            common_telemetry::error!("PostgreSQL connection closed with an error: {e}");
        }
    });

    let (schema, table) = split_remote_table(remote_table);
    let schema = schema.unwrap_or("public").to_string();
    let table = table.to_string();

    let sql = "SELECT column_name, udt_name, numeric_precision, numeric_scale, datetime_precision \
               FROM information_schema.columns \
               WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position";

    let rows = client
        .query(sql, &[&schema, &table])
        .await
        .map_err(|e| box_foreign_error(e))
        .context(FetchMetadataSnafu)?;

    ensure!(
        !rows.is_empty(),
        RemoteTableNotFoundSnafu {
            table: remote_table.to_string(),
        }
    );

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let udt_name: String = row.get(1);
        let precision: Option<i32> = row.get(2);
        let scale: Option<i32> = row.get(3);
        let fsp: Option<i32> = row.get(4);

        let column_type = PostgresColumnType {
            type_name: udt_name,
            precision: precision.map(|v| v as u64),
            scale: scale.map(|v| v as i64),
            fsp: fsp.map(|v| v as u8),
        };
        columns.push(RelationalColumn {
            name,
            data_type: map_type(&column_type)?,
        });
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_types() {
        assert_eq!(
            map_type(&PostgresColumnType::new("int2")).unwrap(),
            ConcreteDataType::int16_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("int4")).unwrap(),
            ConcreteDataType::int32_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("int8")).unwrap(),
            ConcreteDataType::int64_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("float4")).unwrap(),
            ConcreteDataType::float32_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("float8")).unwrap(),
            ConcreteDataType::float64_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("numeric").with_decimal(12, 2)).unwrap(),
            ConcreteDataType::decimal128_datatype(12, 2)
        );
        // Arbitrary precision (no `p`) and `p > 38` are rejected.
        assert!(map_type(&PostgresColumnType::new("numeric")).is_err());
        assert!(map_type(&PostgresColumnType::new("numeric").with_decimal(100, 2)).is_err());
    }

    #[test]
    fn test_string_and_binary() {
        assert_eq!(
            map_type(&PostgresColumnType::new("text")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("varchar")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("bpchar")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("name")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("bytea")).unwrap(),
            ConcreteDataType::binary_datatype()
        );
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(
            map_type(&PostgresColumnType::new("date")).unwrap(),
            ConcreteDataType::date_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("timestamp")).unwrap(),
            ConcreteDataType::timestamp_microsecond_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("timestamptz")).unwrap(),
            ConcreteDataType::timestamp_microsecond_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("timestamp").with_fsp(0)).unwrap(),
            ConcreteDataType::timestamp_second_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("time")).unwrap(),
            ConcreteDataType::time_microsecond_datatype()
        );
        assert!(map_type(&PostgresColumnType::new("timetz")).is_err());
        assert_eq!(
            map_type(&PostgresColumnType::new("interval")).unwrap(),
            ConcreteDataType::interval_month_day_nano_datatype()
        );
    }

    #[test]
    fn test_arrays() {
        assert_eq!(
            map_type(&PostgresColumnType::new("_int4")).unwrap(),
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::int32_datatype()))
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("_text")).unwrap(),
            ConcreteDataType::list_datatype(Arc::new(ConcreteDataType::string_datatype()))
        );
        // An array whose element is unmappable is rejected.
        assert!(map_type(&PostgresColumnType::new("_tsvector")).is_err());
    }

    #[test]
    fn test_json_uuid_and_rejected() {
        assert_eq!(
            map_type(&PostgresColumnType::new("json")).unwrap(),
            ConcreteDataType::json_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("jsonb")).unwrap(),
            ConcreteDataType::json_datatype()
        );
        assert_eq!(
            map_type(&PostgresColumnType::new("uuid")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert!(map_type(&PostgresColumnType::new("money")).is_err());
        assert!(map_type(&PostgresColumnType::new("inet")).is_err());
        assert!(map_type(&PostgresColumnType::new("tsvector")).is_err());
    }
}
