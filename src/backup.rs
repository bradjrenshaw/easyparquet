//This is a prototype and will be significantly optimized
//Currently only prints table values; here for record/prototype purposes
use std::error::Error;
use mysql_async::prelude::*;
use mysql_async::Row;
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

        for column in stream.columns().iter() {
          let col = Column::from_mysql_type(column.column_type())?;
          columns.push(col);
        }

          while let Some(row_result) = stream.next().await {
      let row: Row = row_result?;
      for (i, v) in row.unwrap().into_iter().enumerate() {
        Column::push(&mut columns[i], v)?;
      }
          }

          
        Ok(())
    }
}
