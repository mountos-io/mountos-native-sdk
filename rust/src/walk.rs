//! Recursive listing built on `Client::ls`, so every entry arrives with its attributes and no
//! per-entry stat is needed.

use crate::client::Client;
use crate::error::Result;
use crate::wire::{DirEntry, FileType};

/// Entries requested per `ls` page.
const WALK_PAGE: u32 = 1000;

fn join(dir: &str, name: &str) -> String {
  format!("{}/{name}", dir.trim_end_matches('/'))
}

impl Client {
  /// Lists one directory completely by following the `ls` cursor.
  pub fn read_dir_all(&self, path: &str) -> Result<Vec<DirEntry>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    loop {
      let (entries, next) = self.ls(path, WALK_PAGE, &cursor)?;
      out.extend(entries);
      if next.is_empty() {
        return Ok(out);
      }
      cursor = next;
    }
  }

  /// Visits every entry below `path`, depth first. `f` gets the directory path and the entry,
  /// which carries its attributes. The entries of a directory arrive in listing order and each
  /// subdirectory is visited after the directory that holds it. Symlinks are reported and not
  /// followed. The walk stops at the first error from `ls` or from `f` and returns it. No entry
  /// is skipped silently.
  pub fn walk(&self, path: &str, mut f: impl FnMut(&str, &DirEntry) -> Result<()>) -> Result<()> {
    let mut stack = vec![path.to_string()];
    while let Some(dir) = stack.pop() {
      let mut subdirs = Vec::new();
      let mut cursor = String::new();
      loop {
        let (entries, next) = self.ls(&dir, WALK_PAGE, &cursor)?;
        for e in &entries {
          f(&dir, e)?;
          if e.r#type == FileType::Directory as i32 {
            subdirs.push(join(&dir, &e.name));
          }
        }
        if next.is_empty() {
          break;
        }
        cursor = next;
      }
      stack.extend(subdirs.into_iter().rev());
    }
    Ok(())
  }
}
