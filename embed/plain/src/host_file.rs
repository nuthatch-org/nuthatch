//! redb over a file the host holds open: storage mode `host` (`embed/README.md`). The host opened it
//! when the guest asked, through `nh_step`; every call on it from then on is a synchronous import.

use std::io;

#[link(wasm_import_module = "nuthatch")]
extern "C" {
    fn store_read(handle: i32, offset: i64, ptr: *mut u8, len: usize) -> i32;
    fn store_write(handle: i32, offset: i64, ptr: *const u8, len: usize) -> i32;
    fn store_len(handle: i32) -> i64;
    fn store_set_len(handle: i32, len: i64) -> i32;
    fn store_sync(handle: i32) -> i32;
    fn store_close(handle: i32);
}

/// The most one import call moves, so a length always fits the `i32` it returns.
const CHUNK: usize = 1 << 30;

#[derive(Debug)]
pub struct HostFile {
    handle: i32,
    name: String,
}

impl HostFile {
    pub fn new(handle: i32, name: &str) -> HostFile {
        HostFile {
            handle,
            name: name.to_string(),
        }
    }

    fn failed(&self, call: &str, code: i64) -> io::Error {
        io::Error::other(format!("host storage: {call} on {} gave {code}", self.name))
    }
}

impl Drop for HostFile {
    fn drop(&mut self) {
        unsafe { store_close(self.handle) }
    }
}

fn offset(at: u64) -> io::Result<i64> {
    i64::try_from(at).map_err(|_| io::Error::other("offset past i64"))
}

impl redb::StorageBackend for HostFile {
    fn len(&self) -> io::Result<u64> {
        let n = unsafe { store_len(self.handle) };
        u64::try_from(n).map_err(|_| self.failed("store_len", n))
    }

    fn read(&self, at: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut done = 0;
        while done < len {
            let want = (len - done).min(CHUNK);
            let n = unsafe {
                store_read(
                    self.handle,
                    offset(at + done as u64)?,
                    buf[done..].as_mut_ptr(),
                    want,
                )
            };
            match n {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "read past the end",
                    ))
                }
                n if n < 0 => return Err(self.failed("store_read", n.into())),
                n => done += n as usize,
            }
        }
        Ok(buf)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        match unsafe { store_set_len(self.handle, offset(len)?) } {
            0 => Ok(()),
            n => Err(self.failed("store_set_len", n.into())),
        }
    }

    fn sync_data(&self, _: bool) -> io::Result<()> {
        match unsafe { store_sync(self.handle) } {
            0 => Ok(()),
            n => Err(self.failed("store_sync", n.into())),
        }
    }

    fn write(&self, at: u64, data: &[u8]) -> io::Result<()> {
        let mut done = 0;
        while done < data.len() {
            let want = (data.len() - done).min(CHUNK);
            let n = unsafe {
                store_write(
                    self.handle,
                    offset(at + done as u64)?,
                    data[done..].as_ptr(),
                    want,
                )
            };
            match n {
                0 => return Err(io::Error::new(io::ErrorKind::WriteZero, "wrote nothing")),
                n if n < 0 => return Err(self.failed("store_write", n.into())),
                n => done += n as usize,
            }
        }
        Ok(())
    }
}
