use std::sync::Arc;

use mysql_async::consts::ColumnType as mysql_column_type;
use mysql_async::Value;
use arrow::array::Array;
use arrow::datatypes::{DataType, Field};

//This is a prototype and will definitely be refactored
pub enum Column {
    String(arrow::array::StringBuilder),
    Int64(arrow::array::Int64Builder)
}

impl Column {

    pub fn finish(self) -> Arc<dyn Array> {
        match self {
        Column::String(mut builder) => Arc::new(builder.finish()),
        Column::Int64(mut builder) => Arc::new(builder.finish())
        }
    }

    pub fn create_arrow_field(column: &Column, name: String, nullable: bool) -> Result<Field, String> {
        match column {
            Column::String(..) => Ok(Field::new(name, DataType::Utf8, nullable)),
            Column::Int64(..) => Ok(Field::new(name, DataType::Int64, nullable)),
        }
    }

    pub fn from_mysql_type(column_type: mysql_column_type) -> Result<Column, String> {
        match column_type {
            mysql_column_type::MYSQL_TYPE_LONG => Ok(Column::Int64(arrow::array::Int64Builder::new())),
            mysql_column_type::MYSQL_TYPE_VAR_STRING => Ok(Column::String(arrow::array::StringBuilder::new())),
            _ => Err("Unsupported column type".to_string())
        }
    }

    pub fn push(column:&mut Column, value: Value) -> Result<(), String> {
        match(column, value) {
            (Column::Int64(builder), Value::Int(value)) => {
                builder.append_value(value);
                Ok(())
            },
            (Column::String(builder), Value::Bytes(value)) => {
                let result = String::from_utf8(value).unwrap();
                builder.append_value(result);                
                Ok(())
            },
            _ => Err("Unknown column/value match".to_string())
        }
    }
}
