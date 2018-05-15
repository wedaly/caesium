use quantile::sketch::{Sketch, BUFCOUNT, BUFSIZE, EPSILON};
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::iter::Iterator;

#[derive(Copy, Clone, Eq)]
struct RankedValue {
    value: u64,
    rank: usize,
}

impl Ord for RankedValue {
    fn cmp(&self, other: &RankedValue) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for RankedValue {
    fn partial_cmp(&self, other: &RankedValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedValue {
    fn eq(&self, other: &RankedValue) -> bool {
        self.value == other.value
    }
}

pub struct SketchQuery {
    count: usize, // num items from the source data stream
    items: Vec<RankedValue>,  // sorted asc by value
}

impl SketchQuery {
    pub fn new(sketch: &Sketch) -> SketchQuery {
        let count = sketch
            .buffer_iter()
            .map(|b| (1 << b.level()) * b.len())
            .sum();
        SketchQuery {
            count: count,
            items: SketchQuery::sorted_items_from_sketch(sketch),
        }
    }

    pub fn query(&self, phi: f64) -> Option<u64> {
        assert!(0.0 < phi && phi < 1.0);
        let target = phi * self.count as f64;
        let mut start = 0;
        let mut end = self.items.len();
        while end - start > 1 {
            let mid = start + (end - start) / 2;
            let rank = self.items[mid].rank as f64;
            if target < rank {
                end = mid;
            } else if target > rank {
                start = mid;
            } else {
                return Some(self.items[mid].value)
            }
        }
        if end - start == 1 {
            Some(self.items[start].value)
        } else {
            None
        }
    }

    fn sorted_items_from_sketch(sketch: &Sketch) -> Vec<RankedValue> {
        let mut items = Vec::with_capacity(BUFCOUNT * BUFSIZE);
        for b in sketch.buffer_iter() {
            let weight = 1 << b.level();
            for value in b.values() {
                items.push(RankedValue {
                    value: *value,
                    rank: weight,  // store the weight here for now to avoid extra allocation
                })
            }
        }
        items.sort_unstable();

        let mut rank = 0;
        for x in items.iter_mut() {
            let weight = x.rank;  // stored weight from earlier
            x.rank = rank;
            rank += weight;
        }
        items
    }
}
