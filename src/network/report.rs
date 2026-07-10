use crate::error::{QuePaxaError, Result};
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) fn write_json_atomic<T: Serialize>(
    value: &T,
    path: impl Into<PathBuf>,
    description: &str,
) -> Result<()> {
    let path = path.into();
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
        QuePaxaError::StorageError(format!("could not encode {description}: {error}"))
    })?;
    let temporary = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            QuePaxaError::StorageError(format!("could not open {description}: {error}"))
        })?;
    file.write_all(&bytes).map_err(|error| {
        QuePaxaError::StorageError(format!("could not write {description}: {error}"))
    })?;
    file.sync_all().map_err(|error| {
        QuePaxaError::StorageError(format!("could not sync {description}: {error}"))
    })?;
    fs::rename(&temporary, &path).map_err(|error| {
        QuePaxaError::StorageError(format!("could not replace {description}: {error}"))
    })?;
    sync_parent(&path, description)
}

fn sync_parent(path: &Path, description: &str) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            QuePaxaError::StorageError(format!("could not sync {description} directory: {error}"))
        })
}
