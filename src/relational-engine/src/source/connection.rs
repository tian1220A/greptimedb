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

//! Connection DSN parsing plus the column descriptor shared by the MySQL and
//! PostgreSQL schema-inference paths.

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use datatypes::prelude::ConcreteDataType;
use url::Url;

use crate::error::{InvalidConnectionSnafu, Result};
use crate::source::dialect::SqlDialectKind;

/// A remote column as reported by the remote database's metadata store, already
/// mapped to a GreptimeDB [`ConcreteDataType`].
#[derive(Debug, Clone, PartialEq)]
pub struct RelationalColumn {
    pub name: String,
    pub data_type: ConcreteDataType,
}

/// Parsed connection information for a remote database.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub kind: SqlDialectKind,
    /// The DSN exactly as provided (kept for drivers that accept a URL directly).
    pub raw: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl ConnectionInfo {
    /// Parses a `mysql://`, `postgres://` or `postgresql://` DSN.
    ///
    /// Note: `user`/`password` are taken as-is (still percent-encoded); DSNs
    /// whose credentials contain reserved characters must be URL-encoded.
    pub fn parse(dsn: &str) -> Result<Self> {
        let url = Url::parse(dsn).map_err(|e| {
            InvalidConnectionSnafu {
                connection: dsn.to_string(),
                message: e.to_string(),
            }
            .build()
        })?;

        let kind = match url.scheme() {
            "mysql" => SqlDialectKind::MySql,
            "postgres" | "postgresql" => SqlDialectKind::Postgres,
            other => {
                return InvalidConnectionSnafu {
                    connection: dsn.to_string(),
                    message: format!("unsupported scheme `{other}`"),
                }
                .fail();
            }
        };

        let host = url
            .host_str()
            .ok_or_else(|| {
                InvalidConnectionSnafu {
                    connection: dsn.to_string(),
                    message: "missing host".to_string(),
                }
                .build()
            })?
            .to_string();

        let default_port = match kind {
            SqlDialectKind::MySql => 3306,
            SqlDialectKind::Postgres => 5432,
        };

        Ok(Self {
            kind,
            raw: dsn.to_string(),
            host,
            port: url.port().unwrap_or(default_port),
            user: url.username().to_string(),
            password: url.password().unwrap_or_default().to_string(),
            database: url.path().trim_start_matches('/').to_string(),
        })
    }
}

/// Boxes a foreign `std::error::Error` into a [`BoxedError`], preserving its
/// message text. External driver errors (mysql_async / tokio-postgres) do not
/// implement GreptimeDB's [`ErrorExt`](common_error::ext::ErrorExt), so they are
/// flattened into a [`PlainError`] before boxing.
pub(crate) fn box_foreign_error(
    e: impl std::error::Error + Send + Sync + 'static,
) -> BoxedError {
    BoxedError::new(PlainError::new(e.to_string(), StatusCode::Internal))
}

/// Splits `remote_table` into an optional schema qualifier and the bare table
/// name: `"schema.table"` → `(Some("schema"), "table")`, `"table"` → `(None, "table")`.
pub fn split_remote_table(remote_table: &str) -> (Option<&str>, &str) {
    match remote_table.rfind('.') {
        Some(i) => (Some(&remote_table[..i]), &remote_table[i + 1..]),
        None => (None, remote_table),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mysql() {
        let info = ConnectionInfo::parse("mysql://root:secret@127.0.0.1:3307/testdb").unwrap();
        assert_eq!(info.kind, SqlDialectKind::MySql);
        assert_eq!(info.host, "127.0.0.1");
        assert_eq!(info.port, 3307);
        assert_eq!(info.user, "root");
        assert_eq!(info.password, "secret");
        assert_eq!(info.database, "testdb");
    }

    #[test]
    fn test_parse_postgres_default_port() {
        let info = ConnectionInfo::parse("postgresql://user@localhost/mydb").unwrap();
        assert_eq!(info.kind, SqlDialectKind::Postgres);
        assert_eq!(info.port, 5432);
        assert_eq!(info.database, "mydb");
    }

    #[test]
    fn test_parse_rejects_unknown_scheme() {
        assert!(ConnectionInfo::parse("http://localhost/db").is_err());
        assert!(ConnectionInfo::parse("not-a-url").is_err());
    }

    #[test]
    fn test_split_remote_table() {
        assert_eq!(split_remote_table("public.city"), (Some("public"), "city"));
        assert_eq!(split_remote_table("city"), (None, "city"));
    }
}
