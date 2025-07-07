use std::path::PathBuf;
use anyhow::Result;
use serde::Serialize;
use crate::data::{Account, Data, User};

pub fn generate_set<T>(path: &PathBuf, name: String, rows: usize) -> Result<()> 
where
T: Data + Serialize
{
    let mut path = path.clone();
    path.push(name);
    let mut writer = csv::Writer::from_path(path)?;
    for _ in 0..rows {
        //We use rows here to prevent any foreign key constraints failing due to an id being referenced that is higher than rows
        let record = T::generate(rows);
        writer.serialize(record)?;
    }
    writer.flush()?;

    Ok(())
}

pub fn generate(path: PathBuf, rows: usize) -> Result<()> {
    generate_set::<User>(&path, "Users.csv".to_string(), rows)?;
    generate_set::<Account>(&path, "Accounts.csv".to_string(), rows)?;
    Ok(())
}
