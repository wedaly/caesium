pub fn wildcard_match(candidate: &str, pattern: &str) -> bool {
    // Value at table[i][j] represents
    // whether the string candidate[..i] matches pattern p[..j]
    let mut table = Bitmap::new(candidate.len() + 1, pattern.len() + 1);

    // Base case 1: empty pattern matches empty str
    unsafe {
        table.set(0, 0, true);
    }

    // Base case 2: empty pattern never matches non-empty str
    // No need to set it, since table values default to false

    // Base case 3: only wildcard matches empty string
    for (idx, p) in pattern.chars().enumerate() {
        let j = idx + 1;
        unsafe {
            let b = p == '*' && table.get(0, j - 1);
            table.set(0, j, b);
        }
    }

    // Recursion
    for (c_idx, c) in candidate.chars().enumerate() {
        let i = c_idx + 1;
        for (p_idx, p) in pattern.chars().enumerate() {
            let j = p_idx + 1;
            unsafe {
                let b =
                    // Pattern is a wildcard:
                    // 1) both wildcard and character are consumed
                    // 2) only character is consumed
                    // 3) only wildcard is consumed
                    (p == '*' &&
                        (table.get(i - 1, j - 1) ||
                        table.get(i - 1, j) ||
                        table.get(i, j - 1))) ||

                    // Pattern is exact character match
                    (p == c &&
                        (table.get(i - 1, j - 1)));

                table.set(i, j, b);
            }
        }
    }

    // Answer for full input candidate and pattern
    unsafe { table.get(candidate.len(), pattern.len()) }
}

pub fn exact_prefix(pattern: &str) -> String {
    match pattern.find("*") {
        Some(idx) => pattern.split_at(idx).0,
        None => pattern,
    }.to_string()
}

struct Bitmap {
    n: usize,
    m: usize,
    map: Vec<bool>,
}

impl Bitmap {
    fn new(n: usize, m: usize) -> Bitmap {
        let map = vec![false; n * m];
        Bitmap { n, m, map }
    }

    unsafe fn set(&mut self, i: usize, j: usize, b: bool) {
        debug_assert!(i < self.n && j < self.m);
        let x = self.map.get_unchecked_mut(i * self.m + j);
        *x = b;
    }

    unsafe fn get(&self, i: usize, j: usize) -> bool {
        debug_assert!(i < self.n && j < self.m);
        let x = self.map.get_unchecked(i * self.m + j);
        *x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_matches_empty_candidate() {
        assert_no_match("", "abcd");
        assert_no_match("", "ab*d");
        assert_match("", "*");
        assert_match("", "");
    }

    #[test]
    fn it_matches_empty_pattern() {
        assert_no_match("foo", "");
    }

    #[test]
    fn it_matches_exact() {
        assert_match("foo", "foo");
        assert_no_match("foo", "foobar");
        assert_no_match("foo", "barfoo");
        assert_no_match("foo", "f");
        assert_no_match("foo", "fo");
    }

    #[test]
    fn it_matches_wildcard() {
        assert_match("", "*");
        assert_match("foo", "*");
        assert_match("bar", "*");
    }

    #[test]
    fn it_matches_one_wildcard_start() {
        assert_match("foo", "*foo");
        assert_match("foo", "*oo");
        assert_match("foo", "*o");
        assert_no_match("bar", "*foo");
        assert_no_match("bar", "*oo");
        assert_no_match("bar", "*o");
    }

    #[test]
    fn it_matches_one_wildcard_middle() {
        assert_match("foobar", "f*bar");
        assert_match("foobar", "foo*r");
        assert_no_match("foo", "f*bar");
        assert_no_match("bar", "f*bar");
    }

    #[test]
    fn it_matches_one_wildcard_end() {
        assert_match("foobar", "f*");
        assert_match("foobar", "foo*");
        assert_match("foobar", "foobar*");
        assert_no_match("foo", "foob*");
        assert_no_match("bar", "foo*");
    }

    #[test]
    fn it_matches_two_wildcards_nonadjacent() {
        assert_match("foobarbaz", "f*bar*az");
        assert_match("foobarbaz", "*f*bar*az*");
        assert_no_match("foo", "f*bar*az");
        assert_no_match("foo", "*f*bar*az*");
    }

    #[test]
    fn it_matches_two_wildcards_adjacent() {
        assert_match("foobar", "**bar");
        assert_match("foobar", "f**r");
        assert_match("foobar", "foo**");
        assert_match("foobar", "**");
        assert_no_match("foo", "**bar");
        assert_no_match("foo", "f**r");
    }

    #[test]
    fn it_extracts_prefix_from_pattern() {
        assert_prefix("", "");
        assert_prefix("foo", "foo");
        assert_prefix("foo*", "foo");
        assert_prefix("foo*bar", "foo");
        assert_prefix("*bar", "");
    }

    fn assert_no_match(candidate: &str, pattern: &str) {
        println!(
            "assert no match for candidate '{}' using pattern '{}'",
            candidate, pattern
        );
        assert!(!wildcard_match(candidate, pattern));
    }

    fn assert_match(candidate: &str, pattern: &str) {
        println!(
            "assert match for candidate '{}' using pattern '{}'",
            candidate, pattern
        );
        assert!(wildcard_match(candidate, pattern));
    }

    fn assert_prefix(pattern: &str, expected: &str) {
        println!("assert prefix for pattern '{}'", pattern);
        let prefix = exact_prefix(pattern);
        assert_eq!(prefix, expected);
    }
}
