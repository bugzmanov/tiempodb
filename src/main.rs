#![allow(dead_code)]

mod ingest;
mod protocol;
mod storage;
mod wal;

#[cfg(test)]
mod protocol_fuzz;

fn main() {
    println!("Hello, world!");
}
