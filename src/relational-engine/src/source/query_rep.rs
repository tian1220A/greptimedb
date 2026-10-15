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

use crate::source::dialect::SqlDialectKind;
use crate::source::param::SqlParam;

/// A compiled query representation.
///
/// Values are carried apart from the SQL structure so they are always sent as
/// bound parameters, never inlined into the statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryRep {
    /// Parameterized remote SQL: `stmt` holds structure and placeholders only,
    /// `params` holds the values in placeholder order, and `dialect` tells the
    /// executor which driver / placeholder syntax to use.
    Sql {
        dialect: SqlDialectKind,
        stmt: String,
        params: Vec<SqlParam>,
    },
    /// A serialized physical plan, used to query another GreptimeDB instance.
    /// Placeholder for now; filled in by a later commit.
    Plan { plan: Vec<u8> },
}

impl QueryRep {
    pub fn sql(dialect: SqlDialectKind, stmt: String, params: Vec<SqlParam>) -> Self {
        Self::Sql {
            dialect,
            stmt,
            params,
        }
    }

    pub fn plan(plan: Vec<u8>) -> Self {
        Self::Plan { plan }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_variant() {
        let rep = QueryRep::sql(
            SqlDialectKind::Postgres,
            "SELECT 1".to_string(),
            vec![SqlParam::Int64(1)],
        );
        assert_eq!(
            rep,
            QueryRep::Sql {
                dialect: SqlDialectKind::Postgres,
                stmt: "SELECT 1".to_string(),
                params: vec![SqlParam::Int64(1)],
            }
        );
    }

    #[test]
    fn test_plan_variant() {
        let rep = QueryRep::plan(vec![1, 2, 3]);
        assert_eq!(
            rep,
            QueryRep::Plan {
                plan: vec![1, 2, 3]
            }
        );
    }
}
