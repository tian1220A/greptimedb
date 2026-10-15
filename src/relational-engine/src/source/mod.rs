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

//! Abstraction layer for the relational (external table) engine: a
//! parameterized-SQL query representation plus the translator that turns a
//! DataFusion filter [`Expr`](datafusion_expr::Expr) into it, with the pushdown
//! whitelist policy fixed here.

pub mod connection;
pub mod dialect;
pub mod mysql;
pub mod param;
pub mod postgres;
pub mod query_rep;
pub mod translator;
pub mod type_mapper;

use crate::error::Result;

pub use connection::{split_remote_table, ConnectionInfo, RelationalColumn};
pub use dialect::{MySqlDialect, PostgresDialect, SqlDialect, SqlDialectKind};
pub use mysql::{map_type as map_mysql_type, MySqlColumnType, MySqlSource};
pub use param::SqlParam;
pub use postgres::{map_type as map_postgres_type, PostgresColumnType};
pub use query_rep::QueryRep;
pub use translator::{ColumnTypeMap, SqlTranslator};
pub use type_mapper::TypeMapper;

/// Connects to the remote database identified by `connection` and returns the
/// column list of `remote_table`, already mapped to GreptimeDB types.
pub async fn fetch_columns(
    connection: &ConnectionInfo,
    remote_table: &str,
) -> Result<Vec<RelationalColumn>> {
    match connection.kind {
        SqlDialectKind::MySql => mysql::fetch_columns(connection, remote_table).await,
        SqlDialectKind::Postgres => postgres::fetch_columns(connection, remote_table).await,
    }
}
