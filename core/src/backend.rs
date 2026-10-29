//! A redb backend over one byte buffer (RFC-0074's), the tab's fallback where a worker has no sync
//! access handle, and the one the Node gate runs on. [`crate::opfs`] is the one a tab persists on.

use std::io;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct Bytes(Arc<Mutex<Vec<u8>>>);

impl Bytes {
    pub fn from_vec(v: Vec<u8>) -> Bytes {
        Bytes(Arc::new(Mutex::new(v)))
    }

    /// The file as it stands, consistent between write transactions.
    pub fn snapshot(&self) -> Vec<u8> {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl redb::StorageBackend for Bytes {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.lock().len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let b = self.lock();
        let start = usize::try_from(offset).map_err(|_| past_end())?;
        let end = start
            .checked_add(len)
            .filter(|e| *e <= b.len())
            .ok_or_else(past_end)?;
        Ok(b[start..end].to_vec())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let len = usize::try_from(len).map_err(|_| too_large())?;
        self.lock().resize(len, 0);
        Ok(())
    }

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut b = self.lock();
        let start = usize::try_from(offset).map_err(|_| too_large())?;
        let end = start.checked_add(data.len()).ok_or_else(too_large)?;
        if end > b.len() {
            b.resize(end, 0);
        }
        b[start..end].copy_from_slice(data);
        Ok(())
    }
}

fn past_end() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end")
}

fn too_large() -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        "offset past this target's address space",
    )
}

#[cfg(test)]
mod tests {
    use super::Bytes;
    use crate::redb_store::Store;

    #[test]
    fn a_store_written_to_bytes_reopens_from_its_snapshot() {
        let bytes = Bytes::default();
        let db = redb::Builder::new()
            .create_with_backend(bytes.clone())
            .unwrap();
        let store: Store = Store::from_db(db, None).unwrap();
        store.set_meta("last_block", "7").unwrap();
        store.put_entity("7-0", r#"{"block_hash":"0x07"}"#).unwrap();
        drop(store);

        let copy = Bytes::from_vec(bytes.snapshot());
        let db = redb::Builder::new().create_with_backend(copy).unwrap();
        let store: Store = Store::from_db(db, None).unwrap();
        assert_eq!(store.get_meta("last_block").unwrap().as_deref(), Some("7"));
        assert_eq!(store.count().unwrap(), 1);
    }
}
