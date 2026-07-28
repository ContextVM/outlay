//! outlay-shim library root: re-exports modules so integration tests can import
//! them. The bin (`main.rs`) is a thin wrapper over this.

pub mod config;
pub mod conn;
pub mod nip11;
pub mod path;
pub mod server;
pub mod translate;
pub mod transport;
