use quantile::sketch::{Buffer, Sketch, BUFCOUNT, BUFSIZE};
use rand;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BinaryHeap;

struct HeapItem {
    level: usize,
    values: Vec<u64>,
}

impl HeapItem {
    fn new(b: &Buffer) -> HeapItem {
        debug_assert!(b.len() > 0);
        let mut v = Vec::with_capacity(b.len());
        v.extend_from_slice(b.sorted_values());
        HeapItem {
            level: b.level(),
            values: v,
        }
    }
}

impl Eq for HeapItem {}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        // order desc by level, so max heap will prioritize lowest levels first
        self.level.cmp(&other.level).reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &HeapItem) -> bool {
        self.level == other.level
    }
}

pub struct SketchMerger {
    heap: BinaryHeap<HeapItem>,
}

impl SketchMerger {
    pub fn new() -> SketchMerger {
        SketchMerger {
            heap: BinaryHeap::with_capacity(BUFCOUNT * 2),
        }
    }

    pub fn merge(&mut self, mut s1: Sketch, s2: Sketch) -> Sketch {
        self.heap.clear();
        self.insert_into_heap(&s1);
        self.insert_into_heap(&s2);
        self.compact_heap();
        self.update_sketch(&mut s1);
        s1
    }

    fn insert_into_heap(&mut self, s: &Sketch) {
        for b in s.buffer_iter() {
            if b.len() > 0 {
                self.heap.push(HeapItem::new(b));
            }
        }
    }

    fn compact_heap(&mut self) {
        while self.heap.len() > BUFCOUNT {
            if let (Some(head), Some(next)) = (self.heap.pop(), self.heap.pop()) {
                if head.level < next.level {
                    self.heap.push(SketchMerger::compact_one(head));
                } else {
                    if head.values.len() + next.values.len() > BUFSIZE {
                        self.heap.push(SketchMerger::compact_two(head, next));
                    } else {
                        self.heap.push(SketchMerger::concat_two(head, next));
                    }
                }
            }
        }
    }

    fn update_sketch(&mut self, s: &mut Sketch) {
        debug_assert!(self.heap.len() <= BUFCOUNT);
        s.reset();
        for (idx, item) in self.heap.iter().enumerate() {
            let b = s.buffer_mut(idx);
            b.set(item.level, &item.values);
        }
    }

    fn compact_one(mut x: HeapItem) -> HeapItem {
        SketchMerger::compact_sorted_vec(&mut x.values);
        x.level += 1;
        x
    }

    fn compact_two(mut x: HeapItem, y: HeapItem) -> HeapItem {
        let mut tmp = Vec::with_capacity(BUFSIZE * 2);
        SketchMerger::concat_sorted_vectors(&x.values, &y.values, &mut tmp);
        SketchMerger::compact_sorted_vec(&mut tmp);
        x.values.clear();
        x.values.extend_from_slice(&tmp);
        x.level += 1;
        x
    }

    fn compact_sorted_vec(v: &mut Vec<u64>) {
        let r = rand::random::<bool>();
        let n = v.len();
        for idx in 0..n {
            if r == ((idx % 2) == 0) {
                v[idx / 2] = v[idx];
            }
        }
        v.truncate(n / 2);
    }

    fn concat_two(mut x: HeapItem, y: HeapItem) -> HeapItem {
        x.values.extend_from_slice(&y.values);
        x.values.sort_unstable();
        x
    }

    fn concat_sorted_vectors(x: &Vec<u64>, y: &Vec<u64>, out: &mut Vec<u64>) {
        let (mut i, mut j) = (0, 0);
        let (n, m) = (x.len(), y.len());
        while i < n && j < m {
            let (v1, v2) = (x[i], y[j]);
            if v1 < v2 {
                out.push(v1);
                i += 1;
            } else {
                out.push(v2);
                j += 1;
            }
        }

        out.extend_from_slice(&x[i..n]);
        out.extend_from_slice(&y[j..m]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_merges_two_empty_sketches() {
        let mut merger = SketchMerger::new();
        let s1 = Sketch::new();
        let s2 = Sketch::new();
        let result = merger.merge(s1, s2);
        assert_levels(&result, &[]);
        assert_levels(&result, &[]);
    }

    #[test]
    fn it_merges_empty_sketch_with_non_empty_sketch() {
        let mut merger = SketchMerger::new();
        let s1 = Sketch::new();
        let s2 = build_sketch(&[1]);
        let result = merger.merge(s1, s2);
        assert_levels(&result, &[1]);
        assert_lengths(&result, &[BUFSIZE]);
    }

    #[test]
    fn it_merges_two_half_full_sketches() {
        let mut merger = SketchMerger::new();
        let s1 = build_sketch(&[1, 2, 3, 4]);
        let s2 = build_sketch(&[1, 2, 3, 4]);
        let result = merger.merge(s1, s2);
        assert_levels(&result, &[1, 1, 2, 2, 3, 3, 4, 4]);
        assert_lengths(&result, &[BUFSIZE; BUFCOUNT]);
    }

    #[test]
    fn it_merges_two_full_sketches_same_levels() {
        let mut merger = SketchMerger::new();
        let s1 = build_sketch(&[0; BUFCOUNT]);
        let s2 = build_sketch(&[0; BUFCOUNT]);
        let result = merger.merge(s1, s2);
        assert_levels(&result, &[1; BUFCOUNT]);
        assert_lengths(&result, &[BUFSIZE; BUFCOUNT]);
    }

    #[test]
    fn it_merges_two_full_sketches_different_levels() {
        let mut merger = SketchMerger::new();
        let levels = [0, 1, 2, 3, 4, 5, 6, 7];
        let s1 = build_sketch(&levels);
        let s2 = build_sketch(&levels);
        let result = merger.merge(s1, s2);
        assert_levels(&result, &[4, 5, 5, 5, 6, 6, 7, 7]);
        assert_lengths(&result, &[64, 240, 256, 256, 256, 256, 256, 256]);
    }

    #[test]
    fn it_compacts_single_item() {
        let level = 1;
        let item = build_heap_item(level, BUFSIZE);
        let result = SketchMerger::compact_one(item);
        assert_eq!(result.level, level + 1);
        assert_eq!(result.values.len(), BUFSIZE / 2);
        match result.values.first() {
            Some(&v) if v == 0 => assert_evens(&result.values),
            Some(&v) if v == 1 => assert_odds(&result.values),
            Some(_) => panic!("First item does not have expected value!"),
            None => panic!("No first item found!"),
        }
    }

    #[test]
    fn it_compacts_two_items() {
        let level = 1;
        let x = build_heap_item(level, BUFSIZE);
        let y = build_heap_item(level, BUFSIZE);
        let result = SketchMerger::compact_two(x, y);
        assert_eq!(result.level, level + 1);
        assert_eq!(result.values.len(), BUFSIZE);
        assert_sequential(&result.values);
    }

    #[test]
    fn it_concats_two_items() {
        let level = 1;
        let x = build_heap_item(level, BUFSIZE / 2);
        let y = build_heap_item(level, BUFSIZE / 2);
        let result = SketchMerger::concat_two(x, y);
        assert_eq!(result.level, level);
        assert_eq!(result.values.len(), BUFSIZE);

        let odds: Vec<u64> = result
            .values
            .iter()
            .enumerate()
            .filter_map(|(idx, v)| if idx % 2 == 1 { Some(*v) } else { None })
            .collect();
        assert_sequential(&odds);

        let evens: Vec<u64> = result
            .values
            .iter()
            .enumerate()
            .filter_map(|(idx, v)| if idx % 2 == 0 { Some(*v) } else { None })
            .collect();
        assert_sequential(&evens);
    }

    fn build_sketch(levels: &[usize]) -> Sketch {
        let data = [0; BUFSIZE];
        let mut s = Sketch::new();
        for (idx, level) in levels.iter().enumerate() {
            s.buffer_mut(idx).set(*level, &data);
        }
        s
    }

    fn build_heap_item(level: usize, len: usize) -> HeapItem {
        let data: Vec<u64> = (0..len).map(|v| v as u64).collect();
        let mut b = Buffer::new();
        b.set(level, data.as_slice());
        HeapItem::new(&b)
    }

    fn assert_levels(s: &Sketch, levels: &[usize]) {
        let mut expected = levels.to_vec();
        expected.sort();

        let mut actual: Vec<usize> = s.buffer_iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.level())
            .collect();
        actual.sort();

        assert_eq!(actual, expected);
    }

    fn assert_lengths(s: &Sketch, lengths: &[usize]) {
        let mut expected = lengths.to_vec();
        expected.sort();

        let mut actual: Vec<usize> = s.buffer_iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.len())
            .collect();
        actual.sort();

        assert_eq!(actual, expected.to_vec());
    }

    fn assert_evens(actual: &[u64]) {
        let evens: Vec<u64> = (0..BUFSIZE / 2).map(|v| (v * 2) as u64).collect();
        assert_eq!(actual.to_vec(), evens);
    }

    fn assert_odds(actual: &[u64]) {
        let odds: Vec<u64> = (0..BUFSIZE / 2).map(|v| ((v * 2) + 1) as u64).collect();
        assert_eq!(actual.to_vec(), odds);
    }

    fn assert_sequential(actual: &[u64]) {
        let expected: Vec<u64> = (0..actual.len()).map(|v| v as u64).collect();
        assert_eq!(actual.to_vec(), expected);
    }
}
