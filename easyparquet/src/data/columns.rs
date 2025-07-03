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

pub struct ColumnData {
    name: String,
    nullable: bool,
    column_type: mysql_column_type,
    arrow_type: DataType,
}

impl ColumnData {
    pub fn get_arrow_type(column_type: mysql_column_type) -> Result<DataType> {
        match column_type {
            mysql_column_type::MYSQL_TYPE_VAR_STRING => Ok(DataType::Utf8),
            mysql_column_type::MYSQL_TYPE_LONG => Ok(DataType::Int64),
            mysql_column_type::MYSQL_TYPE_FLOAT => Ok(DataType::Float32),
            _ => bail!("No matching Arrow schema type for column."),
        }
    }

    pub fn new(name: String, nullable: bool, column_type: mysql_column_type) -> Result<ColumnData> {
        let arrow_type = ColumnData::get_arrow_type(column_type)?;
        Ok(ColumnData {
            name,
            nullable,
            column_type,
            arrow_type,
        })
    }

    pub fn get_schema_field(&self) -> Field {
        Field::new(self.name.clone(), self.arrow_type.clone(), self.nullable)
    }
}

pub struct ColumnHolder<T: ColumnBuilder> {
    data: Arc<ColumnData>,
    builder: T,
}

impl<T: ColumnBuilder> ColumnHolder<T> {
    pub fn new(data: Arc<ColumnData>, builder: T) -> ColumnHolder<T> {
        ColumnHolder { data, builder }
    }

    pub fn finish(self) -> Arc<dyn Array> {
        self.builder.finish()
    }

    pub fn push(&mut self, value: mysql_async::Value) {
        if self.data.nullable {
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
    String(ColumnHolder<StringColumnBuilder>),
    Int64(ColumnHolder<Int64ColumnBuilder>),
    Float(ColumnHolder<FloatColumnBuilder>),
}

impl Column {
    pub fn finish(self) -> Arc<dyn Array> {
        match self {
            Column::String(data) => data.finish(),
            Column::Int64(data) => data.finish(),
            Column::Float(data) => data.finish(),
        }
    }

    pub fn from_data(data: Arc<ColumnData>) -> Result<Column> {
        match data.column_type {
            mysql_column_type::MYSQL_TYPE_LONG => {
                let column = Column::Int64(ColumnHolder::new(data, Int64ColumnBuilder::new()));
                Ok(column)
            }
            mysql_column_type::MYSQL_TYPE_VAR_STRING => {
                let column = Column::String(ColumnHolder::new(data, StringColumnBuilder::new()));
                Ok(column)
            }
            mysql_column_type::MYSQL_TYPE_FLOAT => {
                let column = Column::Float(ColumnHolder::new(data, FloatColumnBuilder::new()));
                Ok(column)
            }
            _ => bail!("Unsupported column type".to_string()),
        }
    }

    pub fn push(column: &mut Column, value: Value) -> Result<()> {
        match column {
            Column::String(holder) => {
                holder.push(value);
                Ok(())
            }
            Column::Int64(holder) => {
                holder.push(value);
                Ok(())
            }
            Column::Float(holder) => {
                holder.push(value);
                Ok(())
            }
        }
    }
}
