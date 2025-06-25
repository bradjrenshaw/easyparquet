//This is a prototype and will be significantly optimized
//Currently only prints table values; here for record/prototype purposes
use std::error::Error;
use mysql_async::prelude::*;
use mysql_async::Row;
use futures::StreamExt;

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

        for column in stream.columns().iter() {
            println!("Column: {}", column.name_str());
        }

          while let Some(row_result) = stream.next().await {
            println!("Row");
      let row: Row = row_result?;
      for v in row.unwrap() {
        println!("{v:?}")
      }
          }
        Ok(())
    }
}
