//! A redb backend over one byte buffer, so the whole store can be handed to the host and back.

use std::io;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Default)]
pub struct Bytes(Arc<Mutex<Vec<u8>>>);

impl Bytes {
    pub fn from_vec(v: Vec<u8>) -> Bytes {
        Bytes(Arc::new(Mutex::new(v)))
    }

    /// The store as it stands. Only consistent between write transactions, which is every moment the
    /// host can ask, since the module runs one call at a time.
    pub fn snapshot(&self) -> Vec<u8> {
        self.0.lock().expect("store lock").clone()
    }
}

impl redb::StorageBackend for Bytes {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.0.lock().expect("store lock").len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let b = self.0.lock().expect("store lock");
        let start = offset as usize;
        let end = start
            .checked_add(len)
            .filter(|e| *e <= b.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end"))?;
        Ok(b[start..end].to_vec())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.0.lock().expect("store lock").resize(len as usize, 0);
        Ok(())
    }

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut b = self.0.lock().expect("store lock");
        let start = offset as usize;
        let end = start + data.len();
        if end > b.len() {
            b.resize(end, 0);
        }
        b[start..end].copy_from_slice(data);
        Ok(())
    }
}
