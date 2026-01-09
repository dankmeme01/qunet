#![allow(clippy::needless_range_loop)]

use criterion::{Criterion, criterion_group, criterion_main};
use qunet::buffers::*;
use std::hint::black_box;

fn exhaust_small_mpsc_pool(c: &mut Criterion) {
    let pool = BufferPool::<MpscInnerPool>::new(1024, 16, 16);
    let mut noise = [0u8; 1024];

    for i in 0..1024 {
        noise[i] = (i % 256) as u8;
    }

    c.bench_function("fill small mpsc", |b| {
        b.iter(|| {
            let mut buf = black_box(pool.try_get().expect("must have buffer"));

            // fill buffer
            black_box(&mut buf).copy_from_slice(black_box(&noise));

            // read back
            for (i, byte) in black_box(&buf[..]).iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        });
    });
}

fn exhaust_small_notify_pool(c: &mut Criterion) {
    let pool = BufferPool::<NotifyInnerPool>::new(1024, 16, 16);
    let mut noise = [0u8; 1024];

    for i in 0..1024 {
        noise[i] = (i % 256) as u8;
    }

    c.bench_function("fill small notify", |b| {
        b.iter(|| {
            let mut buf = black_box(pool.try_get().expect("must have buffer"));

            // fill buffer
            black_box(&mut buf).copy_from_slice(black_box(&noise));

            // read back
            for (i, byte) in black_box(&buf[..]).iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        });
    });
}

fn exhaust_large_mpsc_pool(c: &mut Criterion) {
    let pool = BufferPool::<MpscInnerPool>::new(2048, 4096, 4096);
    let mut noise = [0u8; 2048];

    for i in 0..2048 {
        noise[i] = (i % 256) as u8;
    }

    c.bench_function("fill large mpsc", |b| {
        b.iter(|| {
            let mut buf = black_box(pool.try_get().expect("must have buffer"));

            // fill buffer
            black_box(&mut buf).copy_from_slice(black_box(&noise));

            // read back
            for (i, byte) in black_box(&buf[..]).iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        });
    });
}

fn exhaust_large_notify_pool(c: &mut Criterion) {
    let pool = BufferPool::<NotifyInnerPool>::new(2048, 4096, 4096);
    let mut noise = [0u8; 2048];

    for i in 0..2048 {
        noise[i] = (i % 256) as u8;
    }

    c.bench_function("fill large notify", |b| {
        b.iter(|| {
            let mut buf = black_box(pool.try_get().expect("must have buffer"));

            // fill buffer
            black_box(&mut buf).copy_from_slice(black_box(&noise));

            // read back
            for (i, byte) in black_box(&buf[..]).iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8);
            }
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.05).sample_size(256);
    targets = exhaust_small_mpsc_pool, exhaust_small_notify_pool, exhaust_large_mpsc_pool, exhaust_large_notify_pool
);
criterion_main!(benches);
