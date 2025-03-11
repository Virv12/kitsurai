use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Mutex,
};

use anyhow::{bail, Result};

type Table = BTreeMap<String, Vec<u8>>;

static DATA: Mutex<BTreeMap<String, Table>> = Mutex::new(BTreeMap::new());

pub fn table_create(name: &str) -> Result<()> {
    let mut data = DATA.lock().unwrap();
    match data.entry(name.to_string()) {
        Entry::Vacant(vacant_entry) => vacant_entry.insert(Table::new()),
        Entry::Occupied(occupied_entry) => bail!("table already exists: {}", occupied_entry.key()),
    };
    Ok(())
}

pub fn table_delete(name: &str) -> Result<()> {
    let mut data = DATA.lock().unwrap();
    match data.remove(name) {
        Some(_) => Ok(()),
        None => bail!("table does not exist: {}", name),
    }
}

pub fn item_get(table: &str, key: &str) -> Result<Option<Vec<u8>>> {
    let data = DATA.lock().unwrap();
    let Some(table) = data.get(table) else {
        bail!("table does not exist: {}", table);
    };
    Ok(table.get(key).cloned())
}

pub fn item_set(table: &str, key: &str, value: Vec<u8>) -> Result<()> {
    let mut data = DATA.lock().unwrap();
    let Some(table) = data.get_mut(table) else {
        bail!("table does not exist: {}", table);
    };
    table.insert(key.to_string(), value);
    Ok(())
}

pub fn item_delete(table: &str, key: &str) -> Result<()> {
    let mut data = DATA.lock().unwrap();
    let Some(table) = data.get_mut(table) else {
        bail!("table does not exist: {}", table);
    };
    match table.remove(key) {
        Some(_) => Ok(()),
        None => bail!("key does not exist: {}", key),
    }
}
