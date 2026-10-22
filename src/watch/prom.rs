//! Prometheus text, as much of it as the engine room reads.

pub type Series = (String, Vec<(String, String)>, f64);

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    series: Vec<Series>,
}

impl Metrics {
    pub fn parse(text: &str) -> Self {
        let mut series = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((head, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            let (name, labels) = match head.split_once('{') {
                Some((name, rest)) => (name, parse_labels(rest.strip_suffix('}').unwrap_or(rest))),
                None => (head, Vec::new()),
            };
            series.push((name.to_string(), labels, value));
        }
        Self { series }
    }

    /// The first series called `name` carrying every label in `want`.
    pub fn get(&self, name: &str, want: &[(&str, &str)]) -> Option<f64> {
        self.series
            .iter()
            .find(|(n, labels, _)| {
                n == name
                    && want
                        .iter()
                        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
            })
            .map(|(_, _, v)| *v)
    }

    /// Every value `label` takes on `name`, in the order the exposition lists them.
    pub fn label_values(&self, name: &str, label: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for (n, labels, _) in &self.series {
            if n != name {
                continue;
            }
            for (k, v) in labels {
                if k == label && !out.contains(v) {
                    out.push(v.clone());
                }
            }
        }
        out
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.series.iter().map(|(n, _, _)| n.as_str())
    }
}

fn parse_labels(s: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut chars = s.chars().peekable();
    loop {
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        let key = key.trim_start_matches(',').trim().to_string();
        if key.is_empty() || chars.next() != Some('"') {
            break;
        }
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    if let Some(next) = chars.next() {
                        value.push(if next == 'n' { '\n' } else { next });
                    }
                }
                '"' => break,
                c => value.push(c),
            }
        }
        out.push((key, value));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_and_values_parse() {
        let m = Metrics::parse(
            "# HELP x\nnuthatch_rss_bytes 50987008\n\
             nuthatch_rpc_endpoint_requests_total{endpoint=\"eth.drpc.org\"} 6\n\
             nuthatch_nest_last_block{nest=\"a \\\"b\\\"\",x=\"y\"} 7\n",
        );
        assert_eq!(m.get("nuthatch_rss_bytes", &[]), Some(50_987_008.0));
        assert_eq!(
            m.get(
                "nuthatch_rpc_endpoint_requests_total",
                &[("endpoint", "eth.drpc.org")]
            ),
            Some(6.0)
        );
        assert_eq!(
            m.label_values("nuthatch_nest_last_block", "nest"),
            vec!["a \"b\""]
        );
        assert_eq!(
            m.get("nuthatch_nest_last_block", &[("x", "y")]),
            Some(7.0)
        );
    }
}
