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

use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::prelude::ConcreteDataType;

use crate::source::dialect::{MySqlDialect, PostgresDialect, SqlDialect};
use crate::source::param::SqlParam;
use crate::source::query_rep::QueryRep;

/// Column name → type, used by the translator to apply the pushdown whitelist.
pub type ColumnTypeMap = HashMap<String, ConcreteDataType>;

/// Translates a DataFusion filter [`Expr`] into parameterized SQL.
///
/// The pushdown whitelist policy is fixed here:
/// - `IS [NOT] NULL` on any column: always pushed.
/// - a simple comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`) between a column and
///   a literal: pushed only when the column is not a string-like type.
/// - `IN` / `NOT IN` over a list of literals: pushed only for non-string-like
///   columns.
/// - anything else: not pushed (`None`), left for local evaluation.
///
/// Literal values are never inlined into the SQL: they become bound parameters
/// (`?` for MySQL, `$n` for PostgreSQL) collected by the translator.
pub struct SqlTranslator {
    dialect: Box<dyn SqlDialect>,
    params: Vec<SqlParam>,
}

impl SqlTranslator {
    pub fn new(dialect: Box<dyn SqlDialect>) -> Self {
        Self {
            dialect,
            params: Vec::new(),
        }
    }

    pub fn mysql() -> Self {
        Self::new(Box::new(MySqlDialect))
    }

    pub fn postgres() -> Self {
        Self::new(Box::new(PostgresDialect))
    }

    /// Translates a filter expression into a SQL predicate string with
    /// placeholders, or `None` when the expression is not pushable.
    pub fn translate_filter(&mut self, expr: &Expr, types: &ColumnTypeMap) -> Option<String> {
        self.translate_expr(expr, types)
    }

    /// The bound parameters collected so far, in placeholder order.
    pub fn params(&self) -> &[SqlParam] {
        &self.params
    }

    /// Consumes the translator and wraps the accumulated params into a
    /// [`QueryRep`] for the given (already parameterized) SQL statement.
    pub fn into_query_rep(self, stmt: String) -> QueryRep {
        QueryRep::sql(self.dialect.kind(), stmt, self.params)
    }

    fn translate_expr(&mut self, expr: &Expr, types: &ColumnTypeMap) -> Option<String> {
        match expr {
            Expr::Column(column) => Some(column.name.clone()),
            Expr::IsNull(inner) => {
                let inner = self.translate_expr(inner, types)?;
                Some(format!("{inner} IS NULL"))
            }
            Expr::IsNotNull(inner) => {
                let inner = self.translate_expr(inner, types)?;
                Some(format!("{inner} IS NOT NULL"))
            }
            Expr::BinaryExpr(binary) => self.translate_binary(binary, types),
            Expr::InList(in_list) => self.translate_in_list(in_list, types),
            _ => None,
        }
    }

    fn translate_binary(&mut self, binary: &BinaryExpr, types: &ColumnTypeMap) -> Option<String> {
        match binary.op {
            Operator::And | Operator::Or => {
                let left = self.translate_expr(binary.left.as_ref(), types)?;
                let right = self.translate_expr(binary.right.as_ref(), types)?;
                Some(format!("({left}) {} ({right})", binary.op))
            }
            op if Self::is_comparison(op) => {
                let (column, value) =
                    Self::split_column_literal(binary.left.as_ref(), binary.right.as_ref())?;
                if !Self::is_comparison_pushable(column, types) {
                    return None;
                }
                let param = Self::literal_to_param(value)?;
                let placeholder = self.push_param(param);
                Some(format!("{} {} {}", column.name, op, placeholder))
            }
            _ => None,
        }
    }

    fn translate_in_list(&mut self, in_list: &InList, types: &ColumnTypeMap) -> Option<String> {
        let column = match in_list.expr.as_ref() {
            Expr::Column(column) => column,
            _ => return None,
        };
        if !Self::is_comparison_pushable(column, types) {
            return None;
        }

        let mut placeholders = Vec::with_capacity(in_list.list.len());
        for item in &in_list.list {
            let param = Self::literal_to_param(item)?;
            placeholders.push(self.push_param(param));
        }

        let op = if in_list.negated { "NOT IN" } else { "IN" };
        Some(format!(
            "{} {} ({})",
            column.name,
            op,
            placeholders.join(", ")
        ))
    }

    fn push_param(&mut self, param: SqlParam) -> String {
        self.params.push(param);
        self.dialect.placeholder(self.params.len())
    }

    fn is_comparison(op: Operator) -> bool {
        matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        )
    }

    /// Normalizes either `col op lit` or `lit op col` into `(col, lit)`.
    fn split_column_literal<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a Column, &'a Expr)> {
        match (left, right) {
            (Expr::Column(column), literal) => Some((column, literal)),
            (literal, Expr::Column(column)) => Some((column, literal)),
            _ => None,
        }
    }

    /// A comparison is pushable only when the column type is known and is not
    /// string-like; unknown types fall back to local evaluation.
    fn is_comparison_pushable(column: &Column, types: &ColumnTypeMap) -> bool {
        match types.get(&column.name) {
            Some(data_type) => !Self::is_string_like(data_type),
            None => false,
        }
    }

    fn is_string_like(data_type: &ConcreteDataType) -> bool {
        matches!(
            data_type,
            ConcreteDataType::String(_) | ConcreteDataType::Binary(_) | ConcreteDataType::Json(_)
        )
    }

    /// Extracts a bindable parameter from a literal; typed NULLs are not
    /// bindable and yield `None`.
    fn literal_to_param(expr: &Expr) -> Option<SqlParam> {
        let Expr::Literal(scalar, _) = expr else {
            return None;
        };
        match scalar {
            ScalarValue::Boolean(Some(v)) => Some(SqlParam::Bool(*v)),
            ScalarValue::Int8(Some(v)) => Some(SqlParam::Int8(*v)),
            ScalarValue::Int16(Some(v)) => Some(SqlParam::Int16(*v)),
            ScalarValue::Int32(Some(v)) => Some(SqlParam::Int32(*v)),
            ScalarValue::Int64(Some(v)) => Some(SqlParam::Int64(*v)),
            ScalarValue::UInt8(Some(v)) => Some(SqlParam::UInt8(*v)),
            ScalarValue::UInt16(Some(v)) => Some(SqlParam::UInt16(*v)),
            ScalarValue::UInt32(Some(v)) => Some(SqlParam::UInt32(*v)),
            ScalarValue::UInt64(Some(v)) => Some(SqlParam::UInt64(*v)),
            ScalarValue::Float32(Some(v)) => Some(SqlParam::Float32(*v)),
            ScalarValue::Float64(Some(v)) => Some(SqlParam::Float64(*v)),
            ScalarValue::Utf8(Some(v)) => Some(SqlParam::String(v.clone())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col, lit};
    use datatypes::prelude::ConcreteDataType;

    use super::{ColumnTypeMap, SqlTranslator};
    use crate::source::dialect::SqlDialectKind;
    use crate::source::param::SqlParam;
    use crate::source::query_rep::QueryRep;

    fn types() -> ColumnTypeMap {
        HashMap::from([
            ("host".to_string(), ConcreteDataType::string_datatype()),
            ("val".to_string(), ConcreteDataType::int64_datatype()),
        ])
    }

    fn translate_mysql(expr: &Expr) -> (String, Vec<SqlParam>) {
        let mut translator = SqlTranslator::mysql();
        let sql = translator
            .translate_filter(expr, &types())
            .expect("expression should be pushable");
        (sql, translator.params().to_vec())
    }

    #[test]
    fn test_comparison_mysql_placeholder() {
        let (sql, params) = translate_mysql(&col("val").gt(lit(42i64)));
        assert_eq!(sql, "val > ?");
        assert_eq!(params, vec![SqlParam::Int64(42)]);
    }

    #[test]
    fn test_and_or_with_postgres_placeholders() {
        let expr = col("val").eq(lit(10i64)).and(col("val").not_eq(lit(20i64)));
        let mut translator = SqlTranslator::postgres();
        let sql = translator.translate_filter(&expr, &types()).unwrap();
        assert_eq!(sql, "(val = $1) AND (val != $2)");
        assert_eq!(
            translator.params(),
            &[SqlParam::Int64(10), SqlParam::Int64(20)]
        );
    }

    #[test]
    fn test_is_null_always_pushed() {
        let (sql, params) = translate_mysql(&col("host").is_null());
        assert_eq!(sql, "host IS NULL");
        assert!(params.is_empty());
    }

    #[test]
    fn test_string_column_comparison_not_pushed() {
        let mut translator = SqlTranslator::mysql();
        let expr = col("host").eq(lit("foo"));
        assert!(translator.translate_filter(&expr, &types()).is_none());
        assert!(translator.params().is_empty());
    }

    #[test]
    fn test_in_list_mysql() {
        let expr = col("val").in_list(vec![lit(1i64), lit(2i64), lit(3i64)], false);
        let (sql, params) = translate_mysql(&expr);
        assert_eq!(sql, "val IN (?, ?, ?)");
        assert_eq!(
            params,
            vec![SqlParam::Int64(1), SqlParam::Int64(2), SqlParam::Int64(3)]
        );
    }

    #[test]
    fn test_null_literal_not_pushed() {
        let mut translator = SqlTranslator::mysql();
        let expr = col("val").eq(Expr::Literal(ScalarValue::Null, None));
        assert!(translator.translate_filter(&expr, &types()).is_none());
    }

    #[test]
    fn test_query_rep() {
        let mut translator = SqlTranslator::mysql();
        let predicate = translator
            .translate_filter(&col("val").eq(lit(7i64)), &types())
            .unwrap();
        let rep = translator.into_query_rep(format!("SELECT val FROM t WHERE {predicate}"));
        assert_eq!(
            rep,
            QueryRep::sql(
                SqlDialectKind::MySql,
                "SELECT val FROM t WHERE val = ?".to_string(),
                vec![SqlParam::Int64(7)]
            )
        );
    }
}
