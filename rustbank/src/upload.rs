use std::path::PathBuf;
use anyhow::{bail, Result};
use csv_async::AsyncReaderBuilder;
use mysql_async::{prelude::Queryable, Pool, Transaction, TxOpts};
use serde::{Deserialize, Serialize};
use tokio::fs;
use crate::data::{Data, Account, User};
use crate::Config;
use futures::StreamExt;

async fn check_files(paths: &Vec<PathBuf>) -> Result<()> {
    for p in paths {
        let result = fs::try_exists(p).await?;
        if !result {
            bail!(format!("File {} does not exist.", p.to_str().unwrap()));
        }
    }
    Ok(())
}

async fn upload_dataset<T>(path: PathBuf, pool: Pool) -> Result<()>
where
T: Data + Clone + Sync + Serialize + for<'de> Deserialize<'de> + Into<mysql_async::Params>
{
    const BATCH_SIZE: usize = 1000;
    let file = fs::File::open(path).await?;
    let mut conn = pool.get_conn().await?;
    let mut reader = AsyncReaderBuilder::new().create_deserializer(tokio_util::compat::TokioAsyncReadCompatExt::compat(file));             
    let mut records = reader.deserialize::<T>();
    let mut transaction = conn.start_transaction(TxOpts::default()).await?;
    transaction.query_drop(T::create_query()).await?;
    let insert_statement = T::insert_query();
    let mut batch: Vec<T> = Vec::with_capacity(BATCH_SIZE);

    let mut batches = 0;
    while let Some(record) = records.next().await {
        match record {
            Ok(record) => {
                batch.push(record);
                if batch.len() >= BATCH_SIZE {
                    batches = batches + 1;
                    if let Err(e) = upload_batch(&mut transaction, insert_statement.clone(), &mut batch).await {
                        transaction.rollback().await?;
                        bail!(e);
                    }
                }
            },
            Err(e) => {
                transaction.rollback().await?;
                bail!(e);
            }
        }
    }

    if batch.len() > 0 {
                            if let Err(e) = upload_batch(&mut transaction, insert_statement.clone(), &mut batch).await {
                                transaction.rollback().await?;
                                bail!(e);
                            }
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn upload_batch<T>(transaction: &mut Transaction<'_>, statement: String, batch: &mut Vec<T>) -> Result<()>
    where
    T: Data + Serialize + Clone + Sync + Into<mysql_async::Params>                                                        
    {
        let iter = batch.iter();
    transaction.exec_batch(statement, iter).await?;
        batch.clear();
    Ok(())
}

pub async fn upload(config: &Config, path: PathBuf) -> Result<()> {
    const FILES: [&'static str; 2] = ["users.csv", "accounts.csv"];
    let mut paths = Vec::new();
    for f in FILES {
        let mut path = path.clone();
        path.push(f);
        paths.push(path);
    }
    check_files(&paths).await?;
    let pool = Pool::new(config.get_uri().as_str());
    upload_dataset::<User>(paths[0].clone(), pool.clone()).await?;
    upload_dataset::<Account>(paths[1].clone(), pool).await?;
    Ok(())
}
