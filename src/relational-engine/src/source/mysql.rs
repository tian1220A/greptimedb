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

use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime};
use common_base::bytes::{Bytes, StringBytes};
use common_decimal::Decimal128;
use common_error::ext::BoxedError;
use common_recordbatch::error::{ExternalSnafu, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_time::timestamp::{TimeUnit, Timestamp};
use common_time::{Date, time::Time};
use datatypes::prelude::{ConcreteDataType, DataType, LogicalTypeId};
use datatypes::schema::SchemaRef;
use datatypes::value::{OrderedFloat, Value as GreptimeValue};
use datatypes::vectors::Helper;
use futures::SinkExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Value as MySqlValue};
use snafu::{IntoError, ResultExt, ensure};
use store_api::data_source::DataSource;
use store_api::storage::ScanRequest;

use crate::error::{
    ConnectRemoteSnafu, ConvertValueSnafu, FetchMetadataSnafu, QueryRemoteSnafu,
    RemoteTableNotFoundSnafu, Result, UnsupportedSnafu,
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

/// A live data source backed by a remote MySQL table.
///
/// `schema` is the frozen region schema (the remote columns followed by the
/// synthetic `greptime_timestamp` time-index column). `remote_columns` lists the
/// remote column names in `schema` order and `time_index_pos` is the position of
/// the synthetic column. A scan selects only the requested remote columns and
/// fills the synthetic column with its default value.
#[derive(Clone)]
pub struct MySqlSource {
    connection: ConnectionInfo,
    remote_table: String,
    schema: SchemaRef,
    remote_columns: Vec<String>,
    time_index_pos: usize,
}

impl MySqlSource {
    pub fn new(
        connection: ConnectionInfo,
        remote_table: String,
        schema: SchemaRef,
        remote_columns: Vec<String>,
        time_index_pos: usize,
    ) -> Self {
        Self {
            connection,
            remote_table,
            schema,
            remote_columns,
            time_index_pos,
        }
    }

    /// Builds the projected schema and the SELECT column list, then spawns a task
    /// that streams rows from the remote table into an `mpsc` channel. The channel
    /// receiver is returned as a [`SendableRecordBatchStream`].
    fn get_stream_inner(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let num_columns = self.schema.num_columns();
        let projection = request
            .projection
            .unwrap_or_else(|| (0..num_columns).collect::<Vec<_>>());

        let out_schema: SchemaRef = if projection.len() == num_columns {
            self.schema.clone()
        } else {
            Arc::new(
                self.schema
                    .try_project(&projection)
                    .map_err(|e| {
                        ConvertValueSnafu {
                            column: "<projection>".to_string(),
                            reason: e.to_string(),
                        }
                        .build()
                    })?,
            )
        };

        // For each projected column, record its position in the SELECT result
        // (`Some`) or mark it as the synthetic time-index column (`None`).
        let mut select_columns = Vec::new();
        let mut select_positions = Vec::with_capacity(projection.len());
        for &index in &projection {
            if index == self.time_index_pos {
                select_positions.push(None);
            } else {
                let remote_index = if index < self.time_index_pos {
                    index
                } else {
                    index - 1
                };
                select_positions.push(Some(select_columns.len()));
                select_columns.push(self.remote_columns[remote_index].clone());
            }
        }

        let sql = build_select_sql(&select_columns, &self.remote_table, request.limit);

        let (tx, rx) = futures::channel::mpsc::channel(16);
        let source = self.clone();
        let task_schema = out_schema.clone();
        tokio::spawn(async move {
            let mut tx = tx;
            let result = source
                .stream_rows(&sql, &select_positions, &task_schema, tx.clone())
                .await;
            if let Err(e) = result {
                // The receiver may already be gone (query cancelled); ignore a send
                // failure and simply stop.
                let _ = tx.try_send(Err(to_stream_error(e)));
            }
        });

        Ok(Box::pin(RecordBatchStreamWrapper::new(out_schema, rx)))
    }

    async fn stream_rows(
        &self,
        sql: &str,
        select_positions: &[Option<usize>],
        out_schema: &SchemaRef,
        mut tx: futures::channel::mpsc::Sender<StreamItem>,
    ) -> Result<()> {
        let opts = Opts::from_url(self.connection.raw.as_str())
            .map_err(box_foreign_error)
            .context(ConnectRemoteSnafu)?;
        let mut conn = Conn::new(opts)
            .await
            .map_err(box_foreign_error)
            .context(ConnectRemoteSnafu)?;

        let mut result = conn
            .exec_iter(sql, ())
            .await
            .map_err(box_foreign_error)
            .context(QueryRemoteSnafu)?;

        let num_columns = select_positions.len();
        let mut rows: Vec<Vec<GreptimeValue>> = Vec::with_capacity(BATCH_SIZE);

        while let Some(row) = result
            .next()
            .await
            .map_err(box_foreign_error)
            .context(QueryRemoteSnafu)?
        {
            let raw = row.unwrap();
            let mut out_row = Vec::with_capacity(num_columns);
            for (i, position) in select_positions.iter().enumerate() {
                let column_schema = &out_schema.column_schemas()[i];
                let value = match position {
                    Some(pos) => mysql_value_to_greptime(
                        &column_schema.name,
                        &raw[*pos],
                        &column_schema.data_type,
                    )?,
                    None => GreptimeValue::Timestamp(Timestamp::new_millisecond(0)),
                };
                out_row.push(value);
            }
            rows.push(out_row);
            if rows.len() >= BATCH_SIZE {
                let batch = build_batch(out_schema, std::mem::take(&mut rows))?;
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(());
                }
            }
        }

        if !rows.is_empty() {
            let batch = build_batch(out_schema, rows)?;
            if tx.send(Ok(batch)).await.is_err() {
                return Ok(());
            }
        }

        Ok(())
    }
}

impl DataSource for MySqlSource {
    fn get_stream(
        &self,
        request: ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        self.get_stream_inner(request).map_err(BoxedError::new)
    }
}

/// Rows are buffered into batches of this size before being sent downstream.
const BATCH_SIZE: usize = 1024;

type StreamItem = RecordBatchResult<RecordBatch>;

/// Boxes a relational-engine error into the external error carried by
/// [`common_recordbatch::error::Error`], so it can travel through the stream.
fn to_stream_error(error: crate::error::Error) -> common_recordbatch::error::Error {
    ExternalSnafu.into_error(BoxedError::new(error))
}

/// Builds a `SELECT` statement for the given remote columns, optionally limiting
/// the number of rows returned.
fn build_select_sql(select_columns: &[String], remote_table: &str, limit: Option<usize>) -> String {
    // A projection that only needs the synthetic time-index column (or an empty
    // projection, e.g. `COUNT(*)`) selects no remote columns. `SELECT 1` keeps
    // the statement valid and still returns one row per source row.
    let columns = if select_columns.is_empty() {
        "1".to_string()
    } else {
        select_columns
            .iter()
            .map(|c| format!("`{}`", c.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let (schema, table) = split_remote_table(remote_table);
    let from = match schema {
        Some(schema) => format!(
            "`{}`.`{}`",
            schema.replace('`', "``"),
            table.replace('`', "``")
        ),
        None => format!("`{}`", table.replace('`', "``")),
    };
    let mut sql = format!("SELECT {columns} FROM {from}");
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}

/// Converts a binary-protocol MySQL value into a GreptimeDB [`GreptimeValue`] of
/// the target [`ConcreteDataType`].
fn mysql_value_to_greptime(
    column: &str,
    value: &MySqlValue,
    data_type: &ConcreteDataType,
) -> Result<GreptimeValue> {
    if matches!(value, MySqlValue::NULL) {
        return Ok(GreptimeValue::Null);
    }

    match data_type.logical_type_id() {
        LogicalTypeId::Boolean => Ok(GreptimeValue::Boolean(mysql_to_bool(column, value)?)),
        LogicalTypeId::Int8 => {
            let v = mysql_to_i64(column, value)?;
            let v = i8::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for Int8"))
            })?;
            Ok(GreptimeValue::Int8(v))
        }
        LogicalTypeId::Int16 => {
            let v = mysql_to_i64(column, value)?;
            let v = i16::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for Int16"))
            })?;
            Ok(GreptimeValue::Int16(v))
        }
        LogicalTypeId::Int32 => {
            let v = mysql_to_i64(column, value)?;
            let v = i32::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for Int32"))
            })?;
            Ok(GreptimeValue::Int32(v))
        }
        LogicalTypeId::Int64 => Ok(GreptimeValue::Int64(mysql_to_i64(column, value)?)),
        LogicalTypeId::UInt8 => {
            let v = mysql_to_u64(column, value)?;
            let v = u8::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for UInt8"))
            })?;
            Ok(GreptimeValue::UInt8(v))
        }
        LogicalTypeId::UInt16 => {
            let v = mysql_to_u64(column, value)?;
            let v = u16::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for UInt16"))
            })?;
            Ok(GreptimeValue::UInt16(v))
        }
        LogicalTypeId::UInt32 => {
            let v = mysql_to_u64(column, value)?;
            let v = u32::try_from(v).map_err(|_| {
                convert_unsupported(column, &format!("value {v} out of range for UInt32"))
            })?;
            Ok(GreptimeValue::UInt32(v))
        }
        LogicalTypeId::UInt64 => Ok(GreptimeValue::UInt64(mysql_to_u64(column, value)?)),
        LogicalTypeId::Float32 => Ok(GreptimeValue::Float32(OrderedFloat(
            mysql_to_f64(column, value)? as f32,
        ))),
        LogicalTypeId::Float64 => Ok(GreptimeValue::Float64(OrderedFloat(
            mysql_to_f64(column, value)?,
        ))),
        LogicalTypeId::Decimal128 => {
            let bytes = mysql_to_bytes(column, value)?;
            let s = std::str::from_utf8(bytes).map_err(|e| {
                convert_unsupported(column, &format!("invalid decimal encoding: {e}"))
            })?;
            let decimal = s.parse::<Decimal128>().map_err(|e| {
                convert_unsupported(column, &format!("invalid decimal `{s}`: {e}"))
            })?;
            Ok(GreptimeValue::Decimal128(decimal))
        }
        LogicalTypeId::String => {
            let bytes = mysql_to_bytes(column, value)?;
            let s = String::from_utf8(bytes.to_vec())
                .map_err(|e| convert_unsupported(column, &format!("invalid UTF-8: {e}")))?;
            Ok(GreptimeValue::String(StringBytes::from(s)))
        }
        LogicalTypeId::Binary => Ok(GreptimeValue::Binary(Bytes::from(
            mysql_to_bytes(column, value)?.to_vec(),
        ))),
        LogicalTypeId::Json => {
            let bytes = mysql_to_bytes(column, value)?;
            let json: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|e| convert_unsupported(column, &format!("invalid JSON: {e}")))?;
            // GreptimeDB stores `Json` columns as binary JSONB; the underlying
            // vector is a `BinaryVectorBuilder`, so a `Value::Json` cannot be
            // pushed into it. Encode the parsed JSON as JSONB bytes and wrap it
            // as a binary value instead.
            let jsonb = jsonb::Value::from(json).to_vec();
            Ok(GreptimeValue::Binary(Bytes::from(jsonb)))
        }
        LogicalTypeId::Date => {
            let (year, month, day, ..) = mysql_to_datetime(column, value)?;
            let date = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| convert_unsupported(column, &format!("invalid date {year}-{month}-{day}")))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = date.signed_duration_since(epoch).num_days() as i32;
            Ok(GreptimeValue::Date(Date::new(days)))
        }
        LogicalTypeId::TimestampSecond
        | LogicalTypeId::TimestampMillisecond
        | LogicalTypeId::TimestampMicrosecond
        | LogicalTypeId::TimestampNanosecond => {
            let (year, month, day, hour, minute, second, micro) =
                mysql_to_datetime(column, value)?;
            let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
                convert_unsupported(column, &format!("invalid datetime {year}-{month}-{day}"))
            })?;
            let time = NaiveTime::from_hms_micro_opt(hour, minute, second, micro)
                .ok_or_else(|| convert_unsupported(column, "invalid time"))?;
            let utc = date.and_time(time).and_utc();
            let unit = timestamp_unit(data_type);
            let value = match unit {
                TimeUnit::Second => utc.timestamp(),
                TimeUnit::Millisecond => utc.timestamp_millis(),
                TimeUnit::Microsecond => utc.timestamp_micros(),
                TimeUnit::Nanosecond => utc
                    .timestamp_nanos_opt()
                    .ok_or_else(|| convert_unsupported(column, "timestamp out of nanosecond range"))?,
            };
            Ok(GreptimeValue::Timestamp(Timestamp::new(value, unit)))
        }
        LogicalTypeId::TimeSecond
        | LogicalTypeId::TimeMillisecond
        | LogicalTypeId::TimeMicrosecond
        | LogicalTypeId::TimeNanosecond => {
            let (negative, days, hours, minutes, seconds, micro) = mysql_to_time(column, value)?;
            let sign = if negative { -1i64 } else { 1i64 };
            let total_seconds =
                days as i64 * 86_400 + hours as i64 * 3_600 + minutes as i64 * 60 + seconds as i64;
            let nanos = sign * (total_seconds * 1_000_000_000 + micro as i64 * 1_000);
            let unit = time_unit(data_type);
            let value = match unit {
                TimeUnit::Second => nanos / 1_000_000_000,
                TimeUnit::Millisecond => nanos / 1_000_000,
                TimeUnit::Microsecond => nanos / 1_000,
                TimeUnit::Nanosecond => nanos,
            };
            Ok(GreptimeValue::Time(Time::new(value, unit)))
        }
        _ => Err(convert_unsupported(
            column,
            &format!("unsupported target type {}", data_type.name()),
        )),
    }
}

/// Transposes buffered rows into column vectors and builds a single [`RecordBatch`].
fn build_batch(schema: &SchemaRef, rows: Vec<Vec<GreptimeValue>>) -> Result<RecordBatch> {
    let num_columns = schema.num_columns();
    let num_rows = rows.len();
    let mut columns = Vec::with_capacity(num_columns);
    for col in 0..num_columns {
        let column_schema = &schema.column_schemas()[col];
        let mut values = Vec::with_capacity(num_rows);
        for row in &rows {
            values.push(row[col].clone());
        }
        let vector = Helper::try_from_row_into_vector(&values, &column_schema.data_type)
            .map_err(|e| {
                ConvertValueSnafu {
                    column: column_schema.name.clone(),
                    reason: e.to_string(),
                }
                .build()
            })?;
        columns.push(vector);
    }
    RecordBatch::new(schema.clone(), columns).map_err(|e| {
        ConvertValueSnafu {
            column: "<batch>".to_string(),
            reason: e.to_string(),
        }
        .build()
    })
}

fn timestamp_unit(data_type: &ConcreteDataType) -> TimeUnit {
    match data_type {
        ConcreteDataType::Timestamp(t) => t.unit(),
        _ => unreachable!("expected timestamp type, got {data_type:?}"),
    }
}

fn time_unit(data_type: &ConcreteDataType) -> TimeUnit {
    match data_type {
        ConcreteDataType::Time(t) => t.unit(),
        _ => unreachable!("expected time type, got {data_type:?}"),
    }
}

fn convert_unsupported(column: &str, reason: &str) -> crate::error::Error {
    ConvertValueSnafu {
        column: column.to_string(),
        reason: reason.to_string(),
    }
    .build()
}

fn mysql_to_i64(column: &str, value: &MySqlValue) -> Result<i64> {
    match value {
        MySqlValue::Int(i) => Ok(*i),
        MySqlValue::UInt(u) => i64::try_from(*u).map_err(|_| {
            convert_unsupported(
                column,
                &format!("unsigned value {u} out of range for a signed integer"),
            )
        }),
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to a signed integer"),
        )),
    }
}

fn mysql_to_u64(column: &str, value: &MySqlValue) -> Result<u64> {
    match value {
        MySqlValue::UInt(u) => Ok(*u),
        MySqlValue::Int(i) => u64::try_from(*i).map_err(|_| {
            convert_unsupported(
                column,
                &format!("negative value {i} cannot become an unsigned integer"),
            )
        }),
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to an unsigned integer"),
        )),
    }
}

fn mysql_to_f64(column: &str, value: &MySqlValue) -> Result<f64> {
    match value {
        MySqlValue::Float(f) => Ok(*f as f64),
        MySqlValue::Double(d) => Ok(*d),
        MySqlValue::Int(i) => Ok(*i as f64),
        MySqlValue::UInt(u) => Ok(*u as f64),
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to a float"),
        )),
    }
}

fn mysql_to_bool(column: &str, value: &MySqlValue) -> Result<bool> {
    match value {
        MySqlValue::Int(i) => Ok(*i != 0),
        MySqlValue::UInt(u) => Ok(*u != 0),
        MySqlValue::Bytes(bytes) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| convert_unsupported(column, &format!("invalid UTF-8: {e}")))?;
            Ok(s == "1" || s.eq_ignore_ascii_case("true"))
        }
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to a boolean"),
        )),
    }
}

fn mysql_to_bytes<'a>(column: &str, value: &'a MySqlValue) -> Result<&'a [u8]> {
    match value {
        MySqlValue::Bytes(bytes) => Ok(bytes.as_slice()),
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to bytes"),
        )),
    }
}

/// Extracts `(year, month, day, hour, minute, second, micro)` from a
/// binary-protocol `Date` value.
fn mysql_to_datetime(
    column: &str,
    value: &MySqlValue,
) -> Result<(i32, u32, u32, u32, u32, u32, u32)> {
    match value {
        MySqlValue::Date(year, month, day, hour, minute, second, micro) => Ok((
            *year as i32,
            *month as u32,
            *day as u32,
            *hour as u32,
            *minute as u32,
            *second as u32,
            *micro,
        )),
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to a datetime"),
        )),
    }
}

fn mysql_to_time(column: &str, value: &MySqlValue) -> Result<(bool, u32, u8, u8, u8, u32)> {
    match value {
        MySqlValue::Time(negative, days, hours, minutes, seconds, micro) => {
            Ok((*negative, *days, *hours, *minutes, *seconds, *micro))
        }
        other => Err(convert_unsupported(
            column,
            &format!("cannot convert {other:?} to a time"),
        )),
    }
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
