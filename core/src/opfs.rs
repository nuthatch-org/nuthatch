//! redb over one OPFS file through a `FileSystemSyncAccessHandle` (RFC-0075 §4), written from redb's
//! `StorageBackend` docs and the File System Standard. Worker-only: no other global has the handle.
//!
//! The handle is the lock. While one is open on a file, a second `createSyncAccessHandle` on it, from
//! this tab or any other, rejects with `NoModificationAllowedError`, which [`OpenError::Locked`] names.

use std::io;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    DomException, FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
    FileSystemGetFileOptions, FileSystemReadWriteOptions, FileSystemSyncAccessHandle,
    WorkerGlobalScope,
};

#[cfg(target_feature = "atomics")]
compile_error!("OpfsBackend's Send and Sync rest on a single-threaded module");

/// Why a store could not be opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenError {
    /// Another handle holds the file: a second tab, or a second open in this one.
    Locked {
        file: String,
    },
    /// Not in a worker, or the browser has no OPFS; the byte-buffer backend is the fallback.
    Unavailable(String),
    Js(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Locked { file } => write!(
                f,
                "{file} is open in another tab: a nest has one writer, so this tab will not open it"
            ),
            OpenError::Unavailable(why) => write!(f, "OPFS is not available here: {why}"),
            OpenError::Js(e) => write!(f, "OPFS: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

fn js_error(e: JsValue) -> OpenError {
    match e.dyn_ref::<DomException>() {
        Some(d) => OpenError::Js(format!("{}: {}", d.name(), d.message())),
        None => OpenError::Js(format!("{e:?}")),
    }
}

/// The origin's OPFS root, from the worker's `navigator.storage`.
pub async fn root() -> Result<FileSystemDirectoryHandle, OpenError> {
    let scope = js_sys::global()
        .dyn_into::<WorkerGlobalScope>()
        .map_err(|_| OpenError::Unavailable("not a worker".into()))?;
    let dir = JsFuture::from(scope.navigator().storage().get_directory())
        .await
        .map_err(js_error)?;
    Ok(dir.unchecked_into())
}

/// `name` under `parent`, created if absent: a nest's directory is its NID.
pub async fn directory(
    parent: &FileSystemDirectoryHandle,
    name: &str,
) -> Result<FileSystemDirectoryHandle, OpenError> {
    let opts = FileSystemGetDirectoryOptions::new();
    opts.set_create(true);
    let dir = JsFuture::from(parent.get_directory_handle_with_options(name, &opts))
        .await
        .map_err(js_error)?;
    Ok(dir.unchecked_into())
}

pub struct OpfsBackend {
    handle: FileSystemSyncAccessHandle,
    file: String,
}

// SAFETY: without the atomics target feature (refused above) a wasm module has one thread, so the
// handle is never reached from a second one.
unsafe impl Send for OpfsBackend {}
unsafe impl Sync for OpfsBackend {}

impl std::fmt::Debug for OpfsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpfsBackend")
            .field("file", &self.file)
            .finish()
    }
}

impl OpfsBackend {
    /// Take the sync access handle on `file` in `dir`, creating the file if absent.
    pub async fn open(
        dir: &FileSystemDirectoryHandle,
        file: &str,
    ) -> Result<OpfsBackend, OpenError> {
        let opts = FileSystemGetFileOptions::new();
        opts.set_create(true);
        let fh: FileSystemFileHandle =
            JsFuture::from(dir.get_file_handle_with_options(file, &opts))
                .await
                .map_err(js_error)?
                .unchecked_into();
        let handle = JsFuture::from(fh.create_sync_access_handle())
            .await
            .map_err(|e| match e.dyn_ref::<DomException>() {
                Some(d) if d.name() == "NoModificationAllowedError" => OpenError::Locked {
                    file: file.to_string(),
                },
                _ => js_error(e),
            })?
            .unchecked_into();
        Ok(OpfsBackend {
            handle,
            file: file.to_string(),
        })
    }

    fn at(offset: u64) -> FileSystemReadWriteOptions {
        let opts = FileSystemReadWriteOptions::new();
        opts.set_at_f64(offset as f64);
        opts
    }
}

impl Drop for OpfsBackend {
    fn drop(&mut self) {
        self.handle.close();
    }
}

fn io_error(e: JsValue) -> io::Error {
    io::Error::other(js_error(e).to_string())
}

impl redb::StorageBackend for OpfsBackend {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.handle.get_size().map_err(io_error)? as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let mut buf = vec![0u8; len];
        let mut done = 0;
        while done < len {
            let at = Self::at(offset + done as u64);
            let n = self
                .handle
                .read_with_u8_array_and_options(&mut buf[done..], &at)
                .map_err(io_error)? as usize;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "read past the end",
                ));
            }
            done += n;
        }
        Ok(buf)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.handle.truncate_with_f64(len as f64).map_err(io_error)
    }

    fn sync_data(&self, _: bool) -> Result<(), io::Error> {
        self.handle.flush().map_err(io_error)
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut done = 0;
        while done < data.len() {
            let at = Self::at(offset + done as u64);
            let n = self
                .handle
                .write_with_u8_array_and_options(&data[done..], &at)
                .map_err(io_error)? as usize;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "OPFS wrote nothing",
                ));
            }
            done += n;
        }
        Ok(())
    }
}
