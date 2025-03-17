use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use std::sync::RwLock;

static DATA: RwLock<BTreeMap<String, Vec<u8>>> = RwLock::new(BTreeMap::new());

#[derive(thiserror::Error, Serialize, Deserialize, Debug)]
#[derive(Clone)]
pub(crate) enum Error {}

pub(crate) fn item_get(key: &str) -> Result<Option<Vec<u8>>, Error> {
    let data = DATA.read().expect("Poisoned lock.");
    Ok(data.get(key).cloned())
}

pub(crate) fn item_set(key: &str, value: Vec<u8>) -> Result<(), Error> {
    let mut data = DATA.write().expect("Poisoned lock.");
    data.insert(key.to_string(), value);
    Ok(())
}
