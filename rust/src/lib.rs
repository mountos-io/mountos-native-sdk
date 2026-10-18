//! Blocking client for the mountOS native filesystem API over a Unix socket.
//!
//! ```no_run
//! let c = mountos_native::Client::connect("/home/me/.mountOS/sockets/mountos_api_0123456789abcdef.sock")?;
//! let f = c.open("/a.txt", mountos_native::open_flag::READ_ONLY, 0)?;
//! let data = f.read_at(0, 4096)?;
//! # Ok::<(), mountos_native::Error>(())
//! ```
//!
//! The server pushes notices (warm results, watch changes, closing) on the connection that made
//! the call. A reader thread per connection receives them, and `Client::add_listener` hands every
//! event to your listeners in order. `Client::wait_warm` and `Client::watch` build on the same
//! events. With no listener, events go to the logger in `Options`, or to stderr.

mod bulk;
mod client;
mod dir;
mod error;
mod events;
mod file;
pub mod frame;
mod ops_gen;
mod watch;
pub mod wire;

pub use bulk::StatResult;
pub use client::{Client, ClientOptions, Options};
pub use dir::Dir;
pub use error::{Error, Result};
pub use events::{ChangeKind, Event, EventKind, ListenerId, Logger};
pub use file::{File, WriteItem};
pub use watch::Watch;
pub use wire::{open_flag, DirEntry, FileInfo, FileType, Range, StatfsInfo};
