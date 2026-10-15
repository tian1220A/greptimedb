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

use datatypes::prelude::ConcreteDataType;

use crate::error::{Result, UnsupportedSnafu};

/// Maps GreptimeDB types to a dialect-neutral remote SQL type name.
///
/// The mapping table is fixed here: a type without a remote counterpart is an
/// error. Dialect-specific spellings (e.g. `TEXT` vs `VARCHAR`) are refined in
/// the per-source crates.
#[derive(Debug, Default, Clone, Copy)]
pub struct TypeMapper;

impl TypeMapper {
    pub fn to_sql(&self, data_type: &ConcreteDataType) -> Result<String> {
        let sql = match data_type {
            ConcreteDataType::Boolean(_) => "BOOLEAN",
            ConcreteDataType::Int8(_) => "TINYINT",
            ConcreteDataType::Int16(_) => "SMALLINT",
            ConcreteDataType::Int32(_) => "INT",
            ConcreteDataType::Int64(_) => "BIGINT",
            ConcreteDataType::UInt8(_) => "TINYINT",
            ConcreteDataType::UInt16(_) => "SMALLINT",
            ConcreteDataType::UInt32(_) => "INT",
            ConcreteDataType::UInt64(_) => "BIGINT",
            ConcreteDataType::Float32(_) => "FLOAT",
            ConcreteDataType::Float64(_) => "DOUBLE",
            ConcreteDataType::Decimal128(_) => "DECIMAL",
            ConcreteDataType::Binary(_) => "BLOB",
            ConcreteDataType::String(_) => "TEXT",
            ConcreteDataType::Date(_) => "DATE",
            ConcreteDataType::Timestamp(_) => "TIMESTAMP",
            ConcreteDataType::Time(_) => "TIME",
            _ => {
                return UnsupportedSnafu {
                    operation: format!("map data type {}", data_type),
                }
                .fail();
            }
        };
        Ok(sql.to_string())
    }
}

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;

    use super::TypeMapper;

    #[test]
    fn test_map_common_types() {
        let mapper = TypeMapper;
        assert_eq!(
            mapper.to_sql(&ConcreteDataType::int64_datatype()).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            mapper.to_sql(&ConcreteDataType::string_datatype()).unwrap(),
            "TEXT"
        );
        assert_eq!(
            mapper
                .to_sql(&ConcreteDataType::boolean_datatype())
                .unwrap(),
            "BOOLEAN"
        );
    }

    #[test]
    fn test_unmapped_type_errors() {
        let mapper = TypeMapper;
        assert!(mapper.to_sql(&ConcreteDataType::json_datatype()).is_err());
    }
}
