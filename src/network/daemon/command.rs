use regex::Regex;
use time::clock::Clock;
use time::timestamp::TimeStamp;

#[derive(Debug)]
pub struct InsertCmd {
    metric: String,
    ts: TimeStamp,
    value: u64,
}

impl InsertCmd {
    pub fn parse_from_str(s: &str, clock: &Clock) -> Option<InsertCmd> {
        lazy_static! {
            static ref INSERT_CMD_RE: Regex = Regex::new(
                "(?P<metric>[a-z][a-z0-9]*):(?P<value>[0-9]+)|ms([|].+)?"
            ).expect("Could not compile regex");
        }

        INSERT_CMD_RE.captures(s)
            .and_then(|c| match (c.name("metric"), c.name("value")) {
                (Some(metric_match), Some(value_match)) => value_match
                    .as_str()
                    .parse::<u64>()
                    .ok()
                    .map(|value| InsertCmd {
                        metric: metric_match.as_str().to_string(),
                        ts: clock.now(),
                        value: value,
                    }),
                _ => None,
            })
    }

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn ts(&self) -> TimeStamp {
        self.ts
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::clock::MockClock;

    #[test]
    fn it_parses_insert_cmd() {
        assert_cmd("foo:12345|ms", "foo", 12345);
    }

    #[test]
    fn it_ignores_extra_separators() {
        assert_cmd("foo:12345|ms|@0.1", "foo", 12345);
    }

    #[test]
    fn it_handles_invalid_commands() {
        assert_invalid(&"");
        assert_invalid(&"invalid");
        assert_invalid(&":123|ms");
        assert_invalid(&"foo:|ms");
        assert_invalid(&"foo|ms");
        assert_invalid(&"foo:bar|ms");
        assert_invalid(&"foo|bar|ms");
        assert_invalid(&"foo|123|ms");
    }

    fn assert_cmd(s: &str, expected_metric: &str, expected_val: u64) {
        let clock = MockClock::new(60);
        let cmd = InsertCmd::parse_from_str(s, &clock).expect("Could not parse insert cmd");
        assert_eq!(cmd.metric(), expected_metric);
        assert_eq!(cmd.value(), expected_val);
        assert_eq!(cmd.ts(), 60);
    }

    fn assert_invalid(s: &str) {
        println!("Checking that '{}' is invalid", s);
        let clock = MockClock::new(60);
        let cmd = InsertCmd::parse_from_str(s, &clock);
        assert!(cmd.is_none());
    }
}
