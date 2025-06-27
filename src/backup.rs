//This is a prototype and will be significantly optimized
use std::error::Error;
use std::sync::Arc;
use arrow::array::Array;
use mysql_async::prelude::*;
use mysql_async::Row;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use futures::StreamExt;
mod columns;
use columns::Column;

#[derive(Debug)]
pub struct TableBackup {
    pub table_name: String,
    pub output_file_path: String,
}

impl TableBackup {

    pub async fn execute(&self, pool: mysql_async::Pool) -> Result<(), Box<dyn Error>> {
        let mut conn = pool.get_conn().await?;
        let query = format!("SELECT * FROM {}", self.table_name);
        let mut stream = conn.exec_stream(query, mysql_async::Params::Empty).await?;
      let mut columns = Vec::new();

      let mut schema_vec = Vec::new();
        for column in stream.columns().iter() {
          let col = Column::from_mysql_type(column.column_type())?;
          let name = column.name_str().into_owned();
          let nullable = column.flags().contains(mysql_async::consts::ColumnFlags::NOT_NULL_FLAG);
          let field = Column::create_arrow_field(&col, name, nullable)?;
          schema_vec.push(field);
          columns.push(col);
        }

        let schema = Arc::new(Schema::new(schema_vec));

          while let Some(row_result) = stream.next().await {
      let row: Row = row_result?;
      for (i, v) in row.unwrap().into_iter().enumerate() {
        Column::push(&mut columns[i], v)?;
      }
          }

          let batch_vec: Vec<Arc<dyn Array>> = columns.into_iter().map(|col| col.finish()).collect();

          let batch = arrow::array::RecordBatch::try_new(schema.clone(), batch_vec)?;

          self.write(batch, schema.clone()).await?;
        Ok(())
    }

    pub async fn write(&self, batch: arrow::array::RecordBatch, schema: Arc<Schema>) -> Result<(), String> {
      let file = std::fs::File::create(&self.output_file_path).unwrap();
      tokio::task::spawn_blocking(move || {
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
      }).await.unwrap();
      Ok(())
    }
}
