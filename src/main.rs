#![allow(dead_code)]
#![feature(path_try_exists)]

mod ingest;
mod partition;
mod protocol;
mod storage;
mod wal;

#[cfg(test)]
mod protocol_fuzz;

fn main() {
    println!("Hello, world!");
}
