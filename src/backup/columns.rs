use anyhow::{Result, bail};
use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use mysql_async::Value;
use mysql_async::consts::ColumnType as mysql_column_type;
use std::sync::Arc;

//Enums are used here instead of dyn/fat pointers for performance

pub trait ColumnBuilder {
    fn finish(self) -> Arc<dyn Array>;
    fn push_null(&mut self);
    fn push_value(&mut self, value: Value);
}

pub struct ColumnData<T: ColumnBuilder> {
    nullable: bool,
    builder: T,
}

impl<T: ColumnBuilder> ColumnData<T> {
    pub fn new(nullable: bool, builder: T) -> ColumnData<T> {
        ColumnData { nullable, builder }
    }

    pub fn finish(self) -> Arc<dyn Array> {
        self.builder.finish()
    }

    pub fn push(&mut self, value: mysql_async::Value) {
        if self.nullable {
            if let mysql_async::Value::NULL = value {
                self.builder.push_null();
            }
        }
        self.builder.push_value(value);
    }
}

pub struct StringColumnBuilder {
    builder: arrow::array::StringBuilder,
}

impl StringColumnBuilder {
    fn new() -> StringColumnBuilder {
        StringColumnBuilder {
            builder: arrow::array::StringBuilder::new(),
        }
    }
}

impl ColumnBuilder for StringColumnBuilder {
    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) {
        self.builder.append_null();
    }

    fn push_value(&mut self, value: mysql_async::Value) {
        if let mysql_async::Value::Bytes(value) = value {
            let result = String::from_utf8(value).unwrap();
            self.builder.append_value(result);
        }
    }
}

pub struct Int64ColumnBuilder {
    builder: arrow::array::Int64Builder,
}

impl Int64ColumnBuilder {
    fn new() -> Int64ColumnBuilder {
        Int64ColumnBuilder {
            builder: arrow::array::Int64Builder::new(),
        }
    }
}

impl ColumnBuilder for Int64ColumnBuilder {
    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) {
        self.builder.append_null();
    }

    fn push_value(&mut self, value: mysql_async::Value) {
        if let mysql_async::Value::Int(value) = value {
            self.builder.append_value(value);
        }
    }
}

pub struct FloatColumnBuilder {
    builder: arrow::array::Float32Builder,
}

impl FloatColumnBuilder {
    fn new() -> FloatColumnBuilder {
        FloatColumnBuilder {
            builder: arrow::array::Float32Builder::new(),
        }
    }
}

impl ColumnBuilder for FloatColumnBuilder {
    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) {
        self.builder.append_null();
    }

    fn push_value(&mut self, value: mysql_async::Value) {
        if let mysql_async::Value::Float(value) = value {
            self.builder.append_value(value);
        }
    }
}

pub enum Column {
    String(ColumnData<StringColumnBuilder>),
    Int64(ColumnData<Int64ColumnBuilder>),
    Float(ColumnData<FloatColumnBuilder>),
}

impl Column {
    pub fn finish(self) -> Arc<dyn Array> {
        match self {
            Column::String(data) => data.finish(),
            Column::Int64(data) => data.finish(),
            Column::Float(data) => data.finish(),
        }
    }

    pub fn from_mysql_type(
        name: String,
        nullable: bool,
        column_type: mysql_column_type,
    ) -> Result<(Column, Field)> {
        match column_type {
            mysql_column_type::MYSQL_TYPE_LONG => {
                let column = Column::Int64(ColumnData::new(nullable, Int64ColumnBuilder::new()));
                let field = Field::new(name, DataType::Int64, nullable);
                Ok((column, field))
            }
            mysql_column_type::MYSQL_TYPE_VAR_STRING => {
                let column = Column::String(ColumnData::new(nullable, StringColumnBuilder::new()));
                let field = Field::new(name, DataType::Utf8, nullable);
                Ok((column, field))
            }
            mysql_column_type::MYSQL_TYPE_FLOAT => {
                let column = Column::Float(ColumnData::new(nullable, FloatColumnBuilder::new()));
                let field = Field::new(name, DataType::Float32, nullable);
                Ok((column, field))
            }
            _ => bail!("Unsupported column type".to_string()),
        }
    }

    pub fn push(column: &mut Column, value: Value) -> Result<()> {
        match column {
            Column::String(data) => {
                data.push(value);
                Ok(())
            }
            Column::Int64(data) => {
                data.push(value);
                Ok(())
            }
            Column::Float(data) => {
                data.push(value);
                Ok(())
            }
        }
    }
}
