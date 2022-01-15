#![allow(dead_code)]

mod ingest;
mod protocol;
mod storage;

#[cfg(test)]
mod protocol_fuzz;

fn main() {
    println!("Hello, world!");
}
