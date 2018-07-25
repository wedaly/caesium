use quantile::writable::WritableSketch;
use time::timestamp::TimeStamp;

pub struct MetricState {
    pub metric_name: String,
    pub window_start: TimeStamp,
    pub window_end: TimeStamp,
    pub sketch: WritableSketch,
}

impl MetricState {
    pub fn new(metric_name: &str, ts: TimeStamp, value: u64) -> MetricState {
        let mut sketch = WritableSketch::new();
        sketch.insert(value);
        MetricState {
            metric_name: metric_name.to_string(),
            window_start: ts,
            window_end: ts,
            sketch,
        }
    }
}
