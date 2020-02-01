use mem_table::Value;

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn value_new(value :&str) -> Value {
    Value::new(value)
}

fn value_old(value :&str) -> Value {
    Value::old(value)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Value");

    for s in &["hello world", "6.1234", "123456", "1/2/2020 5:34:45 pm"] {
        group.bench_with_input(BenchmarkId::new("new", s), s, |b, s| b.iter(|| value_new(*s)));
        group.bench_with_input(BenchmarkId::new("old", s), s, |b, s| b.iter(|| value_old(*s)));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
