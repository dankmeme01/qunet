#![allow(clippy::needless_range_loop)]

use criterion::{Criterion, criterion_group, criterion_main};
use std::{hint::black_box, time::Instant};

fn regular_instant(c: &mut Criterion) {
    c.bench_function("regular instant", |b| {
        b.iter(|| {
            black_box(Instant::now());
        });
    });
}

fn coarse_instant(c: &mut Criterion) {
    c.bench_function("coarse instant", |b| {
        b.iter(|| black_box(qunet::transport::lowlevel::coarse_monotonic_timer()));
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.05).sample_size(256);
    targets = regular_instant, coarse_instant
);
criterion_main!(benches);
