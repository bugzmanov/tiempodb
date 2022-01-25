use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::rc::Rc;
use std::sync::Arc;

pub fn criterion_benchmark(c: &mut Criterion) {
    let v = vec![1, 2, 3];
    let ss = Arc::new(v);
    c.bench_function("fib 20", |b| b.iter(|| black_box(ss[0])));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
