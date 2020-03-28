use large_table::{Value, ValueType};

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn value_new(value :&str) -> Value {
    Value::new(value)
}

fn value_with_type(value :&str, value_type :&ValueType) -> Value {
    Value::with_type(value, value_type)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Value");

    // test one of each type
    for s in &[("hello world", ValueType::String), ("6.1234", ValueType::Float), ("123456", ValueType::Integer), ("1/2/2020 5:34:45 pm", ValueType::DateTimeFormat("".to_string()))] {
        group.bench_with_input(BenchmarkId::new("new", s.0), s, |b, s| b.iter(|| value_new(s.0)));
        group.bench_with_input(BenchmarkId::new("old", s.0), s, |b, s| b.iter(|| value_with_type(s.0, &s.1)));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
