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

//! MySQL → GreptimeDB type mapping for the `infer_schema` step of `CREATE
//! EXTERNAL TABLE`.
//!
//! This is the forward direction (remote type → [`ConcreteDataType`]); the
//! reverse direction lives in `source/type_mapper.rs`. Types that cannot be
//! mapped faithfully are rejected at table-creation time (create-time frozen,
//! zero runtime guessing).
//!
//! Mapping decisions (see `类型映射核对表-MySQL.md`):
//! - `BIGINT UNSIGNED` → `UInt64` (2^64-1 fits without overflow).
//! - `DECIMAL(p, s)` with `p > 38` is rejected (no silent precision loss).
//! - `TIME` may exceed 24h in MySQL; we map it to `Time` and accept the
//!   overflow risk (documented).
//! - `TIMESTAMP` timezone semantics are dropped (naive `Timestamp`, aligned
//!   with GreptimeDB's own PostgreSQL server).
//! - `BIT` and the spatial types (`geometry`, `point`, …) are rejected.
//! - `BOOL` is indistinguishable from `TINYINT(1)` in `INFORMATION_SCHEMA`, so
//!   it maps to `Int8`/`UInt8` like any other `tinyint`.

use datatypes::prelude::ConcreteDataType;
use mysql_async::prelude::Queryable;
use snafu::{ResultExt, ensure};

use crate::error::{
    ConnectRemoteSnafu, FetchMetadataSnafu, RemoteTableNotFoundSnafu, Result, UnsupportedSnafu,
};
use crate::source::connection::{
    box_foreign_error, split_remote_table, ConnectionInfo, RelationalColumn,
};

/// A MySQL column type as reported by `INFORMATION_SCHEMA.COLUMNS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MySqlColumnType {
    /// `DATA_TYPE` in lowercase, e.g. `tinyint`, `bigint`, `decimal`, `varchar`.
    pub data_type: String,
    /// Whether `UNSIGNED` is present (from `COLUMN_TYPE`, e.g. `bigint unsigned`).
    pub unsigned: bool,
    /// `NUMERIC_PRECISION` for `decimal` / `numeric`.
    pub precision: Option<u64>,
    /// `NUMERIC_SCALE` for `decimal` / `numeric`.
    pub scale: Option<i64>,
    /// `DATETIME_PRECISION` for `time` / `datetime` / `timestamp`.
    pub fsp: Option<u8>,
}

impl MySqlColumnType {
    pub fn new(data_type: impl Into<String>) -> Self {
        Self {
            data_type: data_type.into(),
            unsigned: false,
            precision: None,
            scale: None,
            fsp: None,
        }
    }

    pub fn with_unsigned(mut self, unsigned: bool) -> Self {
        self.unsigned = unsigned;
        self
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

/// Maps a MySQL column type to a GreptimeDB [`ConcreteDataType`].
pub fn map_type(col: &MySqlColumnType) -> Result<ConcreteDataType> {
    let unsigned = col.unsigned;
    match col.data_type.as_str() {
        "tinyint" => Ok(if unsigned {
            ConcreteDataType::uint8_datatype()
        } else {
            ConcreteDataType::int8_datatype()
        }),
        "smallint" => Ok(if unsigned {
            ConcreteDataType::uint16_datatype()
        } else {
            ConcreteDataType::int16_datatype()
        }),
        "mediumint" => Ok(if unsigned {
            ConcreteDataType::uint32_datatype()
        } else {
            ConcreteDataType::int32_datatype()
        }),
        "int" | "integer" => Ok(if unsigned {
            ConcreteDataType::uint32_datatype()
        } else {
            ConcreteDataType::int32_datatype()
        }),
        "bigint" => Ok(if unsigned {
            ConcreteDataType::uint64_datatype()
        } else {
            ConcreteDataType::int64_datatype()
        }),
        "float" => Ok(ConcreteDataType::float32_datatype()),
        "double" | "real" => Ok(ConcreteDataType::float64_datatype()),
        "decimal" | "numeric" => map_decimal(col.precision, col.scale),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
            Ok(ConcreteDataType::string_datatype())
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            Ok(ConcreteDataType::binary_datatype())
        }
        "date" => Ok(ConcreteDataType::date_datatype()),
        "time" => Ok(map_time(col.fsp)),
        "datetime" | "timestamp" => Ok(map_timestamp(col.fsp)),
        "year" => Ok(ConcreteDataType::int16_datatype()),
        "json" => Ok(ConcreteDataType::json_datatype()),
        other => unsupported(other),
    }
}

fn map_decimal(precision: Option<u64>, scale: Option<i64>) -> Result<ConcreteDataType> {
    // MySQL `DECIMAL` defaults to (10, 0).
    let precision = precision.unwrap_or(10);
    let scale = scale.unwrap_or(0);
    if precision > 38 {
        return unsupported(format!("`DECIMAL({precision}, {scale})` (precision > 38)"));
    }
    if scale < 0 || scale as u64 > precision {
        return unsupported(format!("`DECIMAL({precision}, {scale})` (invalid scale)"));
    }
    Ok(ConcreteDataType::decimal128_datatype(
        precision as u8,
        scale as i8,
    ))
}

fn map_time(fsp: Option<u8>) -> ConcreteDataType {
    // MySQL `TIME`/`DATETIME`/`TIMESTAMP` default to `fsp = 0`.
    match fsp.unwrap_or(0) {
        0 => ConcreteDataType::time_second_datatype(),
        1..=3 => ConcreteDataType::time_millisecond_datatype(),
        _ => ConcreteDataType::time_microsecond_datatype(),
    }
}

fn map_timestamp(fsp: Option<u8>) -> ConcreteDataType {
    match fsp.unwrap_or(0) {
        0 => ConcreteDataType::timestamp_second_datatype(),
        1..=3 => ConcreteDataType::timestamp_millisecond_datatype(),
        _ => ConcreteDataType::timestamp_microsecond_datatype(),
    }
}

fn unsupported(what: impl std::fmt::Display) -> Result<ConcreteDataType> {
    UnsupportedSnafu {
        operation: format!("map MySQL type {what}"),
    }
    .fail()
}

/// Connects to the remote MySQL instance and reads the column list of
/// `remote_table` from `INFORMATION_SCHEMA.COLUMNS`, mapping each column to a
/// GreptimeDB [`ConcreteDataType`] via [`map_type`].
pub async fn fetch_columns(
    connection: &ConnectionInfo,
    remote_table: &str,
) -> Result<Vec<RelationalColumn>> {
    let opts = mysql_async::Opts::from_url(connection.raw.as_str())
        .map_err(|e| box_foreign_error(e))
        .context(ConnectRemoteSnafu)?;
    let mut conn = mysql_async::Conn::new(opts)
        .await
        .map_err(|e| box_foreign_error(e))
        .context(ConnectRemoteSnafu)?;

    let (schema, table) = split_remote_table(remote_table);
    let schema = schema.unwrap_or(&connection.database).to_string();
    let table = table.to_string();

    // CAST the numeric metadata columns to SIGNED so their wire types are
    // uniform (nullable, signed) regardless of the MySQL version.
    let sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, \
               CAST(NUMERIC_PRECISION AS SIGNED), CAST(NUMERIC_SCALE AS SIGNED), \
               CAST(DATETIME_PRECISION AS SIGNED) \
               FROM INFORMATION_SCHEMA.COLUMNS \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";

    let rows: Vec<(String, String, String, Option<i64>, Option<i64>, Option<i64>)> = conn
        .exec(sql, (schema, table))
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
    for (name, data_type, column_type, precision, scale, fsp) in rows {
        let column_type = MySqlColumnType {
            data_type: data_type.to_ascii_lowercase(),
            unsigned: column_type.to_ascii_lowercase().contains("unsigned"),
            precision: precision.map(|v| v as u64),
            scale,
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
    fn test_integer_types() {
        assert_eq!(
            map_type(&MySqlColumnType::new("tinyint")).unwrap(),
            ConcreteDataType::int8_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("tinyint").with_unsigned(true)).unwrap(),
            ConcreteDataType::uint8_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("mediumint")).unwrap(),
            ConcreteDataType::int32_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("bigint")).unwrap(),
            ConcreteDataType::int64_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("bigint").with_unsigned(true)).unwrap(),
            ConcreteDataType::uint64_datatype()
        );
    }

    #[test]
    fn test_float_and_decimal() {
        assert_eq!(
            map_type(&MySqlColumnType::new("float")).unwrap(),
            ConcreteDataType::float32_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("double")).unwrap(),
            ConcreteDataType::float64_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("real")).unwrap(),
            ConcreteDataType::float64_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("decimal").with_decimal(12, 2)).unwrap(),
            ConcreteDataType::decimal128_datatype(12, 2)
        );
        // Precision > 38 is rejected rather than silently degraded.
        assert!(map_type(&MySqlColumnType::new("decimal").with_decimal(65, 30)).is_err());
    }

    #[test]
    fn test_string_and_binary() {
        assert_eq!(
            map_type(&MySqlColumnType::new("varchar")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("char")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("text")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("enum")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("set")).unwrap(),
            ConcreteDataType::string_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("blob")).unwrap(),
            ConcreteDataType::binary_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("binary")).unwrap(),
            ConcreteDataType::binary_datatype()
        );
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(
            map_type(&MySqlColumnType::new("date")).unwrap(),
            ConcreteDataType::date_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("datetime")).unwrap(),
            ConcreteDataType::timestamp_second_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("datetime").with_fsp(3)).unwrap(),
            ConcreteDataType::timestamp_millisecond_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("datetime").with_fsp(6)).unwrap(),
            ConcreteDataType::timestamp_microsecond_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("timestamp")).unwrap(),
            ConcreteDataType::timestamp_second_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("time")).unwrap(),
            ConcreteDataType::time_second_datatype()
        );
        assert_eq!(
            map_type(&MySqlColumnType::new("year")).unwrap(),
            ConcreteDataType::int16_datatype()
        );
    }

    #[test]
    fn test_json_and_rejected() {
        assert_eq!(
            map_type(&MySqlColumnType::new("json")).unwrap(),
            ConcreteDataType::json_datatype()
        );
        assert!(map_type(&MySqlColumnType::new("bit")).is_err());
        assert!(map_type(&MySqlColumnType::new("geometry")).is_err());
        assert!(map_type(&MySqlColumnType::new("point")).is_err());
    }
}
