use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex};

use common::{HasLen, TerminatingWrite};

use super::error::{DeleteError, OpenDirectoryError, OpenReadError, OpenWriteError};
use super::file_watcher::FileWatcher;
use super::FileHandle;
use crate::Directory;

/// A simple file-system Directory implementation.
///
/// Does not do any custom caching and just relies on the OS page cache.
#[derive(Clone)]
pub struct FsDirectory {
    root: PathBuf,
    watcher: Arc<FileWatcher>,
    created: Arc<AtomicBool>,
}

impl std::fmt::Debug for FsDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsDirectory")
            .field("root", &self.root)
            .finish()
    }
}

impl FsDirectory {
    /// Opens a [`FsDirectory`] in a directory.
    ///
    /// Returns an error if the `directory_path` does not
    /// exist or if it is not a directory.
    pub fn open<P: AsRef<Path>>(directory_path: P) -> Result<Self, OpenDirectoryError> {
        // let path = directory_path.as_ref();
        // eprintln!("opening index at {}", path.display());

        // let directory_path: &Path = directory_path.as_ref();
        // if !directory_path.is_dir() {
        //     return Err(OpenDirectoryError::DoesNotExist(PathBuf::from(
        //         directory_path,
        //     )));
        // }

        // Canonicalize not supported on wasm.
        // let canonical_path: PathBuf = directory_path.canonicalize().map_err(|io_err| {
        //     eprintln!("could not canonicalize!");
        //     OpenDirectoryError::wrap_io_error(io_err, PathBuf::from(directory_path))
        // })?;
        // if !canonical_path.is_dir() {
        //     return Err(OpenDirectoryError::NotADirectory(PathBuf::from(
        //         directory_path,
        //     )));
        // }
        let canonical_path = directory_path.as_ref();

        eprintln!("creatign watcher..");
        let watcher = Arc::new(FileWatcher::new(
            &canonical_path.join(*crate::core::META_FILEPATH),
        ));
        dbg!("Index::open success");

        Ok(Self {
            created: Arc::new(AtomicBool::new(canonical_path.exists())),
            root: canonical_path.to_owned(),
            watcher,
        })
    }

    fn subpath(&self, path: impl AsRef<Path>) -> PathBuf {
        self.root.join(path)
    }

    fn ensure_subpath(&self, path: impl AsRef<Path>) -> Result<PathBuf, io::Error> {
        if !self.created.load(atomic::Ordering::SeqCst) {
            std::fs::create_dir_all(&self.root)?;
            self.created.store(true, atomic::Ordering::SeqCst);
        }
        Ok(self.subpath(path))
    }
}

struct FileWriter(File);

impl std::io::Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl TerminatingWrite for FileWriter {
    fn terminate_ref(&mut self, _: common::AntiCallToken) -> io::Result<()> {
        self.0.flush()
    }
}

#[derive(Debug)]
struct FsHandle {
    file: Mutex<File>,
    length: usize,
}

impl HasLen for FsHandle {
    fn len(&self) -> usize {
        self.length
    }
}

impl FileHandle for FsHandle {
    fn read_bytes(&self, range: std::ops::Range<usize>) -> std::io::Result<ownedbytes::OwnedBytes> {
        let mut file = self.file.lock().unwrap();
        file.seek(std::io::SeekFrom::Start(range.start.try_into().unwrap()))?;
        let mut buffer = vec![0u8; range.len()];
        file.read_exact(&mut buffer)?;
        Ok(ownedbytes::OwnedBytes::new(buffer))
    }
}

impl Directory for FsDirectory {
    fn get_file_handle(
        &self,
        path: &std::path::Path,
    ) -> Result<Arc<dyn super::FileHandle>, OpenReadError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.subpath(path))
            .map_err(|err| {
                if err.kind() == io::ErrorKind::NotFound {
                    OpenReadError::FileDoesNotExist(path.to_owned())
                } else {
                    OpenReadError::wrap_io_error(err, path.to_owned())
                }
            })?;

        let length: usize = file
            .metadata()
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_owned()))?
            .len()
            .try_into()
            .unwrap();

        Ok(Arc::new(FsHandle {
            file: Mutex::new(file),
            length,
        }))
    }

    fn delete(&self, path: &std::path::Path) -> Result<(), DeleteError> {
        // TODO: ensure no FsHandle for the path exists?
        std::fs::remove_file(self.subpath(path)).map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                DeleteError::FileDoesNotExist(path.to_owned())
            } else {
                DeleteError::IoError {
                    io_error: Arc::new(err),
                    filepath: path.to_owned(),
                }
            }
        })
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        match std::fs::metadata(dbg!(self.subpath(path))) {
            Ok(m) => dbg!(Ok(m.is_file())),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                dbg!(Ok(false))
            }
            Err(err) => Err(OpenReadError::wrap_io_error(err, path.to_owned())),
        }
    }

    fn open_write(
        &self,
        path: &std::path::Path,
    ) -> Result<super::WritePtr, super::error::OpenWriteError> {
        let full_path = self
            .ensure_subpath(path)
            .map_err(|err| OpenWriteError::wrap_io_error(err, self.root.clone()))?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&full_path)
            .map_err(|err| {
                if err.kind() == io::ErrorKind::AlreadyExists {
                    OpenWriteError::FileAlreadyExists(full_path)
                } else {
                    OpenWriteError::wrap_io_error(err, full_path)
                }
            })?;

        Ok(super::WritePtr::new(Box::new(FileWriter(file))))
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, OpenReadError> {
        std::fs::read(self.subpath(path)).map_err(|err| {
            if err.kind() == io::ErrorKind::NotFound {
                OpenReadError::FileDoesNotExist(path.to_owned())
            } else {
                OpenReadError::wrap_io_error(err, path.to_owned())
            }
        })
    }

    fn atomic_write(&self, path: &std::path::Path, data: &[u8]) -> std::io::Result<()> {
        let path = self.ensure_subpath(path)?;
        eprintln!("atomic write to {}", path.display());
        std::fs::write(&path, data)?;
        eprintln!("file writen: {}", path.display());
        assert!(path.is_file());

        Ok(())
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        // TODO: need anything custom here?
        Ok(())
    }

    fn watch(&self, watch_callback: super::WatchCallback) -> crate::Result<super::WatchHandle> {
        Ok(self.watcher.watch(watch_callback))
    }
}
