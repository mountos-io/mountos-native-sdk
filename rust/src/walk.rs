//! Recursive listing built on `Client::ls`, so every entry arrives with its attributes and no
//! per-entry stat is needed.

use crate::client::Client;
use crate::error::{Error, Result};
use crate::wire::{DirEntry, FileType};

/// Entries requested per `ls` page.
const WALK_PAGE: u32 = 1000;

/// Most entries `read_dir_all` holds.
pub const DEFAULT_MAX_DIR_ENTRIES: usize = 1_000_000;

/// Most directories `walk` keeps queued for later.
pub const DEFAULT_MAX_WALK_PENDING: usize = 65_536;

fn join(dir: &str, name: &str) -> String {
  format!("{}/{name}", dir.trim_end_matches('/'))
}

type Page = (Vec<DirEntry>, String);

fn read_dir_all_with(
  mut ls: impl FnMut(&str, u32, &str) -> Result<Page>,
  path: &str,
  limit: usize,
) -> Result<Vec<DirEntry>> {
  let mut out = Vec::new();
  let mut cursor = String::new();
  loop {
    let (entries, next) = ls(path, WALK_PAGE, &cursor)?;
    if out.len() + entries.len() > limit {
      return Err(Error::TooManyEntries(format!("{path} has more than {limit} entries")));
    }
    out.extend(entries);
    if next.is_empty() {
      return Ok(out);
    }
    cursor = next;
  }
}

fn walk_with(
  mut ls: impl FnMut(&str, u32, &str) -> Result<Page>,
  path: &str,
  max_pending: usize,
  mut f: impl FnMut(&str, &DirEntry) -> Result<()>,
) -> Result<()> {
  let mut stack = vec![path.to_string()];
  while let Some(dir) = stack.pop() {
    let mut subdirs = Vec::new();
    let mut cursor = String::new();
    loop {
      let (entries, next) = ls(&dir, WALK_PAGE, &cursor)?;
      for e in &entries {
        f(&dir, e)?;
        if e.r#type == FileType::Directory as i32 {
          if stack.len() + subdirs.len() >= max_pending {
            return Err(Error::TooManyEntries(format!(
              "walk of {dir} has more than {max_pending} pending directories"
            )));
          }
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

impl Client {
  /// Lists one directory completely by following the `ls` cursor. Fails with
  /// `Error::TooManyEntries` when the directory has more than `DEFAULT_MAX_DIR_ENTRIES` entries.
  pub fn read_dir_all(&self, path: &str) -> Result<Vec<DirEntry>> {
    self.read_dir_all_max(path, DEFAULT_MAX_DIR_ENTRIES)
  }

  /// Like `read_dir_all` with a caller-chosen entry limit.
  pub fn read_dir_all_max(&self, path: &str, max_entries: usize) -> Result<Vec<DirEntry>> {
    read_dir_all_with(|p, n, c| self.ls(p, n, c), path, max_entries)
  }

  /// Visits every entry below `path`, depth first. `f` gets the directory path and the entry,
  /// which carries its attributes. The entries of a directory arrive in listing order and each
  /// subdirectory is visited after the directory that holds it. Symlinks are reported and not
  /// followed. The walk stops at the first error from `ls` or from `f` and returns it. No entry
  /// is skipped silently. The walk holds one page plus the queued directories, at most
  /// `DEFAULT_MAX_WALK_PENDING`, and fails with `Error::TooManyEntries` beyond that.
  pub fn walk(&self, path: &str, f: impl FnMut(&str, &DirEntry) -> Result<()>) -> Result<()> {
    self.walk_max(path, DEFAULT_MAX_WALK_PENDING, f)
  }

  /// Like `walk` with a caller-chosen limit on queued directories.
  pub fn walk_max(&self, path: &str, max_pending: usize, f: impl FnMut(&str, &DirEntry) -> Result<()>) -> Result<()> {
    walk_with(|p, n, c| self.ls(p, n, c), path, max_pending, f)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::cell::Cell;

  fn entry(name: String, dir: bool) -> DirEntry {
    DirEntry {
      name,
      r#type: if dir { FileType::Directory } else { FileType::Regular } as i32,
      info: None,
    }
  }

  /// An endless flat directory served in pages. Counts the calls and the largest page.
  fn endless<'a>(
    calls: &'a Cell<usize>,
    biggest: &'a Cell<usize>,
    dirs: bool,
  ) -> impl FnMut(&str, u32, &str) -> Result<Page> + 'a {
    move |_p, n, c| {
      calls.set(calls.get() + 1);
      let from: usize = c.parse().unwrap_or(0);
      let page: Vec<DirEntry> = (from..from + n as usize)
        .map(|i| entry(format!("n{i}"), dirs))
        .collect();
      biggest.set(biggest.get().max(page.len()));
      Ok((page, (from + n as usize).to_string()))
    }
  }

  fn finite(total: usize, dirs: bool) -> impl FnMut(&str, u32, &str) -> Result<Page> {
    move |_p, n, c| {
      let from: usize = c.parse().unwrap_or(0);
      let end = (from + n as usize).min(total);
      let page: Vec<DirEntry> = (from..end).map(|i| entry(format!("n{i}"), dirs)).collect();
      assert!(page.len() <= WALK_PAGE as usize);
      Ok((page, if end < total { end.to_string() } else { String::new() }))
    }
  }

  #[test]
  fn read_dir_all_stops_at_the_limit() {
    let (calls, biggest) = (Cell::new(0), Cell::new(0));
    let e = read_dir_all_with(endless(&calls, &biggest, false), "/huge", 2500).unwrap_err();
    assert!(matches!(e, Error::TooManyEntries(ref m) if m.contains("2500")));
    assert_eq!(calls.get(), 3);
    assert_eq!(biggest.get(), WALK_PAGE as usize);
  }

  #[test]
  fn read_dir_all_at_the_limit_works() {
    assert_eq!(read_dir_all_with(finite(2500, false), "/d", 2500).unwrap().len(), 2500);
    assert!(read_dir_all_with(finite(2500, false), "/d", 2499).is_err());
  }

  #[test]
  fn walk_bounds_pending_directories() {
    let (calls, biggest) = (Cell::new(0), Cell::new(0));
    let mut visited = 0;
    let e = walk_with(endless(&calls, &biggest, true), "/", 3000, |_, _| {
      visited += 1;
      Ok(())
    })
    .unwrap_err();
    assert!(matches!(e, Error::TooManyEntries(ref m) if m.contains("3000")));
    assert_eq!(visited, 3001);
    assert_eq!(biggest.get(), WALK_PAGE as usize);
    assert_eq!(calls.get(), 4);
  }

  #[test]
  fn walk_holds_one_page_of_a_flat_directory() {
    let mut count = 0;
    walk_with(finite(5003, false), "/f", 4, |_, _| {
      count += 1;
      Ok(())
    })
    .unwrap();
    assert_eq!(count, 5003);
  }
}
