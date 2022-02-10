#![allow(dead_code)]
#![feature(path_try_exists)]

pub mod ingest;
pub mod partition;
mod protocol;
pub mod sql;
pub mod storage;
mod wal;
#[macro_use]
extern crate lalrpop_util;

#[cfg(test)]
#[cfg(not(feature = "no_fuzz"))]
mod protocol_fuzz;
