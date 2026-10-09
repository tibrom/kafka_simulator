use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use chrono::Local;
use serde::Serialize;

#[derive(Clone)]
pub struct Stats(Arc<Mutex<StatsInner>>);

struct StatsInner {
    producers: HashMap<String, ProducerStats>,
    consumers: HashMap<String, ConsumerStats>,
    rebalances: u64,
    errors: u64,
}

struct ProducerStats {
    count_in_window: u64,
    window_start: Instant,
    total: u64,
}

struct ConsumerStats {
    lag: u64,
    total_consumed: u64,
}

#[derive(Serialize)]
struct StatsSummary {
    producers: HashMap<String, ProducerSummary>,
    consumers: HashMap<String, ConsumerSummary>,
    rebalances: u64,
    errors: u64,
    total_produced: u64,
    total_consumed: u64,
}

#[derive(Serialize)]
struct ProducerSummary {
    total_produced: u64,
}

#[derive(Serialize)]
struct ConsumerSummary {
    lag: u64,
    total_consumed: u64,
}

impl Stats {
    pub fn new(producer_ids: &[String], consumer_ids: &[String]) -> Self {
        let now = Instant::now();
        let mut producers = HashMap::new();
        for id in producer_ids {
            producers.insert(
                id.clone(),
                ProducerStats {
                    count_in_window: 0,
                    window_start: now,
                    total: 0,
                },
            );
        }
        let mut consumers = HashMap::new();
        for id in consumer_ids {
            consumers.insert(
                id.clone(),
                ConsumerStats {
                    lag: 0,
                    total_consumed: 0,
                },
            );
        }
        Stats(Arc::new(Mutex::new(StatsInner {
            producers,
            consumers,
            rebalances: 0,
            errors: 0,
        })))
    }

    pub fn record_produced(&self, id: &str) {
        let mut inner = self.0.lock().unwrap();
        if let Some(p) = inner.producers.get_mut(id) {
            p.count_in_window += 1;
            p.total += 1;
        }
    }

    pub fn record_consumed(&self, id: &str) {
        let mut inner = self.0.lock().unwrap();
        if let Some(c) = inner.consumers.get_mut(id) {
            c.total_consumed += 1;
        }
    }

    pub fn record_rebalance(&self) {
        self.0.lock().unwrap().rebalances += 1;
    }

    pub fn record_error(&self) {
        self.0.lock().unwrap().errors += 1;
    }

    pub fn update_lag(&self, id: &str, lag: u64) {
        let mut inner = self.0.lock().unwrap();
        if let Some(c) = inner.consumers.get_mut(id) {
            c.lag = lag;
        }
    }

    pub fn print_report(&self) {
        let ts = Local::now().format("%H:%M:%S");
        let mut inner = self.0.lock().unwrap();
        let now = Instant::now();

        let mut producer_parts = Vec::new();
        let mut total_produced: u64 = 0;
        let mut producer_ids: Vec<String> = inner.producers.keys().cloned().collect();
        producer_ids.sort();
        for id in &producer_ids {
            if let Some(p) = inner.producers.get_mut(id) {
                let elapsed = now.duration_since(p.window_start).as_secs_f64().max(0.001);
                let rate = (p.count_in_window as f64 / elapsed).round() as u64;
                producer_parts.push(format!("{}={}/s", id, rate));
                total_produced += p.total;
                p.count_in_window = 0;
                p.window_start = now;
            }
        }

        let mut consumer_parts = Vec::new();
        let mut total_consumed: u64 = 0;
        let mut consumer_ids: Vec<String> = inner.consumers.keys().cloned().collect();
        consumer_ids.sort();
        for id in &consumer_ids {
            if let Some(c) = inner.consumers.get(id) {
                consumer_parts.push(format!("{} lag={}", id, c.lag));
                total_consumed += c.total_consumed;
            }
        }

        let rebalances = inner.rebalances;
        let errors = inner.errors;

        println!(
            "[kafka-sim] {} | producers: {}",
            ts,
            producer_parts.join("  ")
        );
        println!(
            "[kafka-sim] {} | consumers: {}",
            ts,
            consumer_parts.join("  ")
        );
        println!(
            "[kafka-sim] {} | events: rebalances={}  errors={}  total_produced={}  total_consumed={}",
            ts, rebalances, errors, total_produced, total_consumed
        );
    }

    pub fn to_json(&self) -> String {
        let inner = self.0.lock().unwrap();
        let mut total_produced = 0u64;
        let mut total_consumed = 0u64;

        let producers = inner
            .producers
            .iter()
            .map(|(id, p)| {
                total_produced += p.total;
                (id.clone(), ProducerSummary { total_produced: p.total })
            })
            .collect::<HashMap<_, _>>();

        let consumers = inner
            .consumers
            .iter()
            .map(|(id, c)| {
                total_consumed += c.total_consumed;
                (
                    id.clone(),
                    ConsumerSummary {
                        lag: c.lag,
                        total_consumed: c.total_consumed,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        let summary = StatsSummary {
            producers,
            consumers,
            rebalances: inner.rebalances,
            errors: inner.errors,
            total_produced,
            total_consumed,
        };

        serde_json::to_string_pretty(&summary).unwrap_or_default()
    }
}
