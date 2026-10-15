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

/// Identifies a remote SQL dialect so the executor can pick the right driver
/// and placeholder syntax without inspecting the SQL text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialectKind {
    MySql,
    Postgres,
}

/// SQL dialect of the remote database. Provides the placeholder syntax so the
/// translator can emit `?` (MySQL) or `$n` (PostgreSQL) without hardcoding.
pub trait SqlDialect {
    /// Returns the placeholder for the 1-based `index`-th bound parameter.
    fn placeholder(&self, index: usize) -> String;

    /// The kind of this dialect, carried into [`crate::source::QueryRep`] so
    /// the executor knows which driver to use.
    fn kind(&self) -> SqlDialectKind;
}

/// MySQL-style placeholders: every parameter is `?`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn kind(&self) -> SqlDialectKind {
        SqlDialectKind::MySql
    }
}

/// PostgreSQL-style placeholders: `$1`, `$2`, ...
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn placeholder(&self, index: usize) -> String {
        format!("${index}")
    }

    fn kind(&self) -> SqlDialectKind {
        SqlDialectKind::Postgres
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder() {
        assert_eq!(MySqlDialect.placeholder(3), "?");
        assert_eq!(PostgresDialect.placeholder(3), "$3");
    }

    #[test]
    fn test_kind() {
        assert_eq!(MySqlDialect.kind(), SqlDialectKind::MySql);
        assert_eq!(PostgresDialect.kind(), SqlDialectKind::Postgres);
    }
}
