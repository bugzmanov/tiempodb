use fake::{Dummy, Fake, Faker};
use rand::Rng;
use std::collections::HashMap;
use std::rc::Rc;

mod protocol;
mod storage;

#[cfg(test)]
mod protocol_fuzz;

fn main() {
    println!("Hello, world!");
}
