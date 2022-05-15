use criterion::{black_box, criterion_group, criterion_main, Criterion};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tiempodb::ingest::Engine;
use tiempodb::storage;
use tiempodb::storage::DataPoint;
use tiempodb::storage::StorageWriter;

pub fn ingest_benchmark(c: &mut Criterion) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let tempdir = tempfile::tempdir().unwrap();

    let storage = storage::SnaphotableStorage::new();
    let mut engine = Engine::new(storage, file.path(), tempdir.path()).unwrap();

    let mut data = Vec::with_capacity(1000);
    for i in 0..1000 {
        let line_str =
            format!("weather,location=us-midwest,country=us humidity={i} 146583983010040020{i}");
        data.push(line_str);
    }
    let mut idx = 0;
    let mut group = c.benchmark_group("tiempodb ingest engine");
    // Configure Criterion.rs to detect smaller differences and increase sample size to improve
    // precision and counteract the resulting noise.
    group.significance_level(0.02).sample_size(3000);
    group.bench_function("engine ingest", |b| {
        b.iter(|| {
            black_box(engine.ingest(unsafe { data.get_unchecked(idx) }).unwrap());
            idx += 1;
            if idx >= 1000 {
                idx = 0;
            }
        })
    });

    group.finish();
}

pub fn query_engine_bench(c: &mut Criterion) {
    let snapshot = Arc::new(RwLock::new(HashMap::default()));
    let mut write = snapshot.write();
    (*write).add_bulk(&vec![
        DataPoint::new(Arc::from("table1:metric1"), 100u64, 10f64),
        DataPoint::new(Arc::from("table1:metric1"), 101u64, 12f64),
    ]);

    drop(write);

    let engine = tiempodb::sql::query_engine::QueryEngine::new(snapshot.clone());

    let mut group = c.benchmark_group("tiempodb query engine");

    group.significance_level(0.02).sample_size(3000);
    group.bench_function("engine ingest", |b| {
        b.iter(|| {
            black_box(engine.run_query(
                "SELECT \"metric1\", \"metric2\" FROM \"table1\" WHERE \"host\"=\"localhost\"",
            ).unwrap());
        })
    });

    group.finish();
}

criterion_group!(benches, ingest_benchmark, query_engine_bench);
criterion_main!(benches);
