use anyhow::{Result, bail};
use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use chrono::NaiveDate;
use mysql_async::Value;
use mysql_async::consts::ColumnType as mysql_column_type;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;

//Enums are used here instead of dyn/fat pointers for performance
pub trait ColumnBuilder {
    fn finish(self) -> Arc<dyn Array>;
    fn push_null(&mut self) -> Result<()>;
    fn push_value(&mut self, value: Value) -> Result<()>;
}

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnData {
    name: String,
    unsigned: bool,
    nullable: bool,
    column_type: mysql_column_type,
    arrow_type: DataType,
}

impl ColumnData {
    pub fn get_arrow_type(column_type: mysql_column_type, unsigned: bool) -> Result<DataType> {
        match column_type {
            mysql_column_type::MYSQL_TYPE_VAR_STRING => Ok(DataType::Utf8),
            mysql_column_type::MYSQL_TYPE_LONG | mysql_column_type::MYSQL_TYPE_LONGLONG => {
                if unsigned {
                    Ok(DataType::UInt64)
                } else {
                Ok(DataType::Int64)
                }
            },
            mysql_column_type::MYSQL_TYPE_FLOAT => Ok(DataType::Float32),
            mysql_column_type::MYSQL_TYPE_NEWDECIMAL => Ok(DataType::Decimal128(19, 2)),
            mysql_column_type::MYSQL_TYPE_DATE => Ok(DataType::Date32),
            mysql_column_type::MYSQL_TYPE_DATETIME => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
            _ => bail!("No matching Arrow schema type for column {:?}.", column_type),
        }
    }

    pub fn new(name: String, unsigned: bool, nullable: bool, column_type: mysql_column_type) -> Result<ColumnData> {
        let arrow_type = ColumnData::get_arrow_type(column_type, unsigned)?;
        Ok(ColumnData {
            name,
            unsigned,
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

    pub fn push(&mut self, value: mysql_async::Value) -> Result<()> {
            if let mysql_async::Value::NULL = value {
                if self.data.nullable {
                self.builder.push_null()?;
                return Ok(());
                } else {
                    bail!("Attempted to push null value.");
                }
            }
        self.builder.push_value(value)?;
        Ok(())
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

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Bytes(value) = value {
            let result = String::from_utf8(value).unwrap();
            self.builder.append_value(result);
            Ok(())
        } else {
            bail!("Invalid value");
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

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Int(value) = value {
            self.builder.append_value(value);
            Ok(())
        } else {
            bail!("Value must be an integer.");
        }
    }
}

pub struct Uint64ColumnBuilder {
    builder: arrow::array::UInt64Builder,
}

impl Uint64ColumnBuilder {
    fn new() -> Uint64ColumnBuilder {
        Uint64ColumnBuilder {
            builder: arrow::array::UInt64Builder::new(),
        }
    }
}

impl ColumnBuilder for Uint64ColumnBuilder {
    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        //note: mysql_async uses Int64 to store unsigned longs, etc. Value needs to be converted here.
        if let mysql_async::Value::Int(value) = value {
            self.builder.append_value(value as u64);
            Ok(())
        } else {
            bail!("Value must be an uint but it is {:?}.", value);
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

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Float(value) = value {
            self.builder.append_value(value);
            Ok(())
        } else {
            bail!("Value must be a float.");
        }
    }
}

pub struct DecimalColumnBuilder {
    builder: arrow::array::Decimal128Builder,
    precision: u8,
    scale: i8
}

impl DecimalColumnBuilder {

    fn new(precision: u8, scale: i8) -> DecimalColumnBuilder {
        DecimalColumnBuilder {
            builder: arrow::array::Decimal128Builder::new().with_precision_and_scale(precision, scale).unwrap(),
            precision,
            scale
        }
    }
}

impl ColumnBuilder for DecimalColumnBuilder {

    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Bytes(value) = value {
            let s = str::from_utf8(&value).unwrap();
            let mut dec = Decimal::from_str(&s)?;
            dec.rescale(self.scale as u32);
            let value = dec.mantissa();
            self.builder.append_value(value);
            Ok(())
        } else {
            bail!("Value must be a Decimal.");
        }
    }
}

pub struct DateColumnBuilder {
    builder: arrow::array::Date32Builder,
}

impl DateColumnBuilder {

    pub fn new() -> DateColumnBuilder {
        DateColumnBuilder {
            builder: arrow::array::Date32Builder::new()
        }
    }
}

impl ColumnBuilder for DateColumnBuilder {

    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Date(year, month, day, _, _, _, _) = value {
            let epoch = NaiveDate::from_ymd(1970, 1, 1);
            let dt = NaiveDate::from_ymd(year as i32, month as u32, day as u32);
            let value = dt.signed_duration_since(epoch).num_days() as i32;
            self.builder.append_value(value);
            Ok(())
        } else {
            bail!("Value must be a date.");
        }
    }
}

pub struct DateTimeColumnBuilder {
    builder: arrow::array::TimestampMicrosecondBuilder,
}

impl DateTimeColumnBuilder {

    pub fn new() -> DateTimeColumnBuilder {
        DateTimeColumnBuilder {
            builder: arrow::array::TimestampMicrosecondBuilder::new()
        }
    }
}

impl ColumnBuilder for DateTimeColumnBuilder {

    fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.builder.finish())
    }

    fn push_null(&mut self) -> Result<()> {
        self.builder.append_null();
        Ok(())
    }

    fn push_value(&mut self, value: mysql_async::Value) -> Result<()> {
        if let mysql_async::Value::Date(year, month, day, hours, minutes, seconds, micro_seconds) = value {
            let dt = NaiveDate::from_ymd(year as i32, month as u32, day as u32).and_hms_micro(hours as u32, minutes as u32, seconds as u32, micro_seconds);
            let value = dt.timestamp_micros();
            self.builder.append_value(value);
            Ok(())
        } else {
            bail!("Value msut be a DateTime.");
        }
    }
}

pub enum Column {
    String(ColumnHolder<StringColumnBuilder>),
    Int64(ColumnHolder<Int64ColumnBuilder>),
    Uint64(ColumnHolder<Uint64ColumnBuilder>),
    Float(ColumnHolder<FloatColumnBuilder>),
    Decimal(ColumnHolder<DecimalColumnBuilder>),
    Date(ColumnHolder<DateColumnBuilder>),
    DateTime(ColumnHolder<DateTimeColumnBuilder>),
}

impl Column {
    pub fn finish(self) -> Arc<dyn Array> {
        match self {
            Column::String(data) => data.finish(),
            Column::Int64(data) => data.finish(),
            Column::Uint64(data) => data.finish(),
            Column::Float(data) => data.finish(),
            Column::Decimal(data) => data.finish(),
            Column::Date(data) => data.finish(),
            Column::DateTime(data) => data.finish(),
        }
    }

    pub fn from_data(data: Arc<ColumnData>) -> Result<Column> {
        match data.column_type {
            mysql_column_type::MYSQL_TYPE_LONG | mysql_column_type::MYSQL_TYPE_LONGLONG => {
                if data.unsigned {
                let column = Column::Uint64(ColumnHolder::new(data, Uint64ColumnBuilder::new()));
                    Ok(column)
                } else {
                let column = Column::Int64(ColumnHolder::new(data, Int64ColumnBuilder::new()));
                Ok(column)
                }
            }
            mysql_column_type::MYSQL_TYPE_VAR_STRING => {
                let column = Column::String(ColumnHolder::new(data, StringColumnBuilder::new()));
                Ok(column)
            }
            mysql_column_type::MYSQL_TYPE_FLOAT => {
                let column = Column::Float(ColumnHolder::new(data, FloatColumnBuilder::new()));
                Ok(column)
            },
            mysql_column_type::MYSQL_TYPE_NEWDECIMAL => {
                let column = Column::Decimal(ColumnHolder::new(data, DecimalColumnBuilder::new(19, 2)));
                Ok(column)
            },
            mysql_column_type::MYSQL_TYPE_DATE => {
                let column = Column::Date(ColumnHolder::new(data, DateColumnBuilder::new()));
                Ok(column)
            },
            mysql_column_type::MYSQL_TYPE_DATETIME => {
                let column = Column::DateTime(ColumnHolder::new(data, DateTimeColumnBuilder::new()));
                Ok(column)
            }
            _ => bail!("Unsupported column type".to_string()),
        }
    }

    pub fn push(column: &mut Column, value: Value) -> Result<()> {
        match column {
            Column::String(holder) => {
                holder.push(value)?;
                Ok(())
            }
            Column::Int64(holder) => {
                holder.push(value)?;
                Ok(())
            },
            Column::Uint64(holder) => {
                holder.push(value)?;
                Ok(())
            }
            Column::Float(holder) => {
                holder.push(value)?;
                Ok(())
            },
            Column::Decimal(holder) => {
                holder.push(value)?;
                Ok(())
            },
            Column::Date(holder) => {
                holder.push(value)?;
                Ok(())
            }
            Column::DateTime(holder) => {
                holder.push(value)?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod column_data {
        use super::*;

        use test_case::test_case;

        macro_rules! type_tests {
            ($ ($test_function:item)+ ) => {
                $(
        #[test_case(mysql_column_type::MYSQL_TYPE_VAR_STRING, false, DataType::Utf8; "String type")]
        #[test_case(mysql_column_type::MYSQL_TYPE_LONG, true, DataType::UInt64; "Unsigned Long column type")]
                #[test_case(mysql_column_type::MYSQL_TYPE_LONG, false, DataType::Int64; "Signed Long column type")]
        #[test_case(mysql_column_type::MYSQL_TYPE_LONGLONG, true, DataType::UInt64; "Unsigned long long column type")]
                #[test_case(mysql_column_type::MYSQL_TYPE_LONGLONG, false, DataType::Int64; "Signed long long column type")]
        #[test_case(mysql_column_type::MYSQL_TYPE_FLOAT, false, DataType::Float32; "Float column type")]
        #[test_case(mysql_column_type::MYSQL_TYPE_NEWDECIMAL, false, DataType::Decimal128(19, 2); "Signed decimal column type")]
        #[test_case(mysql_column_type::MYSQL_TYPE_NEWDECIMAL, true, DataType::Decimal128(19, 2); "Unsigned decimal column type")]
        
        $test_function
                )+
            }
        }

        type_tests! {
            fn get_arrow_type_known(column_type: mysql_column_type, unsigned: bool, expected: DataType) {
                assert_eq!(ColumnData::get_arrow_type(column_type, unsigned).unwrap(), expected);
            }
        }

        #[test]
        #[should_panic]
        fn get_arrow_type_unknown() {
            ColumnData::get_arrow_type(mysql_column_type::MYSQL_TYPE_BIT, false).unwrap();
        }

        type_tests! {
            fn new_known_types(column_type: mysql_column_type, unsigned: bool, expected_arrow_type: DataType) {
                let test_data = ColumnData {
                    name: String::from("testing"),
                    unsigned: unsigned,
                    nullable: true,
                    column_type: column_type,
                    arrow_type: expected_arrow_type,
                };
                let data = ColumnData::new(
                    String::from("testing"),
                    unsigned,
                    true,
                    column_type,
                )
                .unwrap();
                assert_eq!(data, test_data);
            }
        }

        #[test]
        #[should_panic]
        fn new_unknown_types() {
            ColumnData::new(
                String::from("testing"),
                false,
                true,
                mysql_column_type::MYSQL_TYPE_BIT,
            )
            .unwrap();
        }

        type_tests! {
            fn get_schema_field(column_type: mysql_column_type, unsigned: bool, expected_arrow_type: DataType) {
                let test_field = Field::new(String::from("testing"), expected_arrow_type, true);
                let data = ColumnData::new(
                    String::from("testing"),
                    unsigned,
                    true,
                column_type
                )
                .unwrap();
                let field = data.get_schema_field();
                assert_eq!(field, test_field);
            }
        }
    }
}
