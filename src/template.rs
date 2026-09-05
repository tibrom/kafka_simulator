use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::SecondsFormat;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use uuid::Uuid;

use crate::config::KeyStrategy;

pub struct MessageRenderer {
    seq: Arc<AtomicU64>,
    rng: StdRng,
    hot_keys: Vec<String>,
    round_robin_counter: Arc<AtomicU64>,
    partition_count: i32,
}

impl MessageRenderer {
    pub fn new(partition_count: i32) -> Self {
        let mut rng = StdRng::from_entropy();
        let hot_keys = (0..20).map(|_| Uuid::new_v4().to_string()).collect();
        let _ = rng.gen::<u8>(); // warm up
        MessageRenderer {
            seq: Arc::new(AtomicU64::new(0)),
            rng,
            hot_keys,
            round_robin_counter: Arc::new(AtomicU64::new(0)),
            partition_count,
        }
    }

    pub fn render_message(&mut self, template: &str) -> String {
        let mut result = String::with_capacity(template.len() * 2);
        let mut remaining = template;

        while let Some(start) = remaining.find("{{") {
            result.push_str(&remaining[..start]);
            remaining = &remaining[start + 2..];

            if let Some(end) = remaining.find("}}") {
                let directive = remaining[..end].trim();
                remaining = &remaining[end + 2..];
                result.push_str(&self.resolve_directive(directive));
            } else {
                // Malformed template — emit as-is
                result.push_str("{{");
                result.push_str(remaining);
                break;
            }
        }

        result.push_str(remaining);
        result
    }

    fn resolve_directive(&mut self, directive: &str) -> String {
        let parts: Vec<&str> = directive.split_whitespace().collect();
        match parts.as_slice() {
            ["uuid"] => Uuid::new_v4().to_string(),
            ["iso_now"] => chrono::Utc::now()
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            ["seq"] => self.seq.fetch_add(1, Ordering::Relaxed).to_string(),
            ["int", min_s, max_s] => {
                if let (Ok(min), Ok(max)) = (min_s.parse::<i64>(), max_s.parse::<i64>()) {
                    self.rng.gen_range(min..=max).to_string()
                } else {
                    "0".to_string()
                }
            }
            ["float", min_s, max_s] => {
                if let (Ok(min), Ok(max)) = (min_s.parse::<f64>(), max_s.parse::<f64>()) {
                    format!("{:.2}", self.rng.gen_range(min..=max))
                } else {
                    "0.00".to_string()
                }
            }
            ["repeat", n_s, char_s] => {
                if let Ok(n) = n_s.parse::<usize>() {
                    char_s.repeat(n)
                } else {
                    String::new()
                }
            }
            parts if parts.first() == Some(&"choice") && parts.len() > 1 => {
                let choices = &parts[1..];
                let idx = self.rng.gen_range(0..choices.len());
                choices[idx].to_string()
            }
            _ => format!("{{{{{}}}}}", directive),
        }
    }

    pub fn compute_key(
        &mut self,
        strategy: &KeyStrategy,
        key_field: Option<&str>,
        rendered: &str,
    ) -> String {
        match strategy {
            KeyStrategy::None => String::new(),
            KeyStrategy::Random => Uuid::new_v4().to_string(),
            KeyStrategy::Field => {
                if let Some(field) = key_field {
                    extract_field(rendered, field)
                        .unwrap_or_else(|| Uuid::new_v4().to_string())
                } else {
                    Uuid::new_v4().to_string()
                }
            }
            KeyStrategy::RoundRobin => {
                let n = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
                (n % self.partition_count as u64).to_string()
            }
            KeyStrategy::Skewed => {
                if self.rng.gen::<f64>() < 0.8 {
                    let idx = self.rng.gen_range(0..self.hot_keys.len());
                    self.hot_keys[idx].clone()
                } else {
                    Uuid::new_v4().to_string()
                }
            }
        }
    }
}

fn extract_field(json_str: &str, field: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(json_str).ok()?;
    match &v[field] {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}
