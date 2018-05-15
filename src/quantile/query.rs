use quantile::sketch::{Sketch, BUFCOUNT, BUFSIZE, EPSILON};
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::iter::Iterator;

#[derive(Copy, Clone, Eq)]
struct WeightedValue {
    value: u64,
    weight: usize,
}

impl Ord for WeightedValue {
    fn cmp(&self, other: &WeightedValue) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for WeightedValue {
    fn partial_cmp(&self, other: &WeightedValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for WeightedValue {
    fn eq(&self, other: &WeightedValue) -> bool {
        self.value == other.value
    }
}

pub struct SketchQuery {
    count: usize, // num items from the source data stream

    // asc by value
    sorted_items: Vec<WeightedValue>,
}

impl SketchQuery {
    pub fn new(sketch: &Sketch) -> SketchQuery {
        let count = sketch
            .buffer_iter()
            .map(|b| (1 << b.level()) * b.len())
            .sum();
        SketchQuery {
            count: count,
            sorted_items: SketchQuery::sorted_items_from_sketch(sketch),
        }
    }

    pub fn query(&self, phi: f64) -> Option<u64> {
        assert!(0.0 < phi && phi < 1.0);

        // Algorithm terminates early once it finds a value in the error bounds
        // so if phi > 0.5 we reverse the order and search for (1 - phi)
        // to find the quantile sooner.
        if phi <= 0.5 {
            SketchQuery::search_items(phi, self.count, self.sorted_items.iter())
        } else {
            SketchQuery::search_items(1.0 - phi, self.count, self.sorted_items.iter().rev())
        }
    }

    fn search_items<'a, I>(phi: f64, n: usize, item_iter: I) -> Option<u64>
    where
        I: Iterator<Item = &'a WeightedValue>,
    {
        let error_bound = EPSILON * n as f64;
        let target = phi * n as f64;
        let mut closest: Option<(f64, u64)> = None;
        let mut rank = 0;
        for &WeightedValue { value, weight } in item_iter {
            rank += weight;
            let error = (rank as f64 - target).abs();
            if error < error_bound {
                return Some(value);
            }

            closest = match closest {
                None => Some((error, value)),
                Some((old_error, _)) if error < old_error => Some((error, value)),
                Some(c) => Some(c),
            }
        }
        closest.map(|(_, val)| val)
    }

    fn sorted_items_from_sketch(sketch: &Sketch) -> Vec<WeightedValue> {
        let mut items = Vec::with_capacity(BUFCOUNT * BUFSIZE);
        for b in sketch.buffer_iter() {
            let weight = 1 << b.level();
            for value in b.values() {
                items.push(WeightedValue {
                    value: *value,
                    weight: weight,
                })
            }
        }
        items.sort_unstable();
        items
    }
}
