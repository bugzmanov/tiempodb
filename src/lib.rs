#![allow(dead_code)]
#![feature(path_try_exists)]
mod ingest;
pub mod partition;
mod protocol;
pub mod storage;
mod wal;

#[cfg(test)]
#[cfg(not(feature = "no_fuzz"))]
mod protocol_fuzz;
