use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, StreamConsumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{BehaviorConfig, BehaviorType, ConnectionConfig, ConsumerGroupConfig};
use crate::stats::Stats;

pub struct ConsumerGroup {
    pub config: ConsumerGroupConfig,
    pub conn: Arc<ConnectionConfig>,
    pub stats: Stats,
    pub behavior: Arc<RwLock<BehaviorConfig>>,
    pub prefix: String,
    member_tokens: Mutex<Vec<CancellationToken>>,
    member_handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ConsumerGroup {
    pub fn new(
        config: ConsumerGroupConfig,
        conn: Arc<ConnectionConfig>,
        stats: Stats,
        prefix: String,
    ) -> Self {
        let behavior = config.behavior.clone();
        ConsumerGroup {
            config,
            conn,
            stats,
            behavior: Arc::new(RwLock::new(behavior)),
            prefix,
            member_tokens: Mutex::new(Vec::new()),
            member_handles: Mutex::new(Vec::new()),
        }
    }

    pub async fn spawn_initial_members(
        self: Arc<Self>,
        parent_token: CancellationToken,
        start_rx: watch::Receiver<bool>,
    ) {
        for i in 0..self.config.members {
            self.clone()
                .spawn_member(i, parent_token.clone(), start_rx.clone())
                .await;
        }
    }

    pub async fn spawn_member(
        self: Arc<Self>,
        member_id: usize,
        parent_token: CancellationToken,
        start_rx: watch::Receiver<bool>,
    ) {
        let token = parent_token.child_token();
        let token_clone = token.clone();
        let group = self.clone();

        let handle = tokio::spawn(async move {
            run_member_with_respawn(group, member_id, token_clone, start_rx).await;
        });

        self.member_tokens.lock().await.push(token);
        self.member_handles.lock().await.push(handle);
    }

    pub async fn add_members(
        self: Arc<Self>,
        count: usize,
        parent_token: CancellationToken,
        start_rx: watch::Receiver<bool>,
    ) {
        let base_id = self.member_tokens.lock().await.len();
        for i in 0..count {
            self.clone()
                .spawn_member(base_id + i, parent_token.clone(), start_rx.clone())
                .await;
        }
        info!(
            group_id = %self.config.id,
            "Added {} members (total: {})", count, base_id + count
        );
    }

    pub async fn remove_members(&self, count: usize) {
        let mut tokens = self.member_tokens.lock().await;
        let actual = count.min(tokens.len());
        for _ in 0..actual {
            if let Some(token) = tokens.pop() {
                token.cancel();
            }
        }
        info!(
            group_id = %self.config.id,
            "Removed {} members (remaining: {})", actual, tokens.len()
        );
    }

    pub async fn wait_all(&self) {
        let mut handles = self.member_handles.lock().await;
        for h in handles.drain(..) {
            h.await.ok();
        }
    }

    pub fn spawn_lag_poller(
        self: Arc<Self>,
        token: CancellationToken,
    ) -> JoinHandle<()> {
        let group = self.clone();
        tokio::spawn(async move {
            run_lag_poller(group, token).await;
        })
    }
}

async fn run_member_with_respawn(
    group: Arc<ConsumerGroup>,
    member_id: usize,
    token: CancellationToken,
    mut start_rx: watch::Receiver<bool>,
) {
    let _ = start_rx.wait_for(|v| *v).await;

    loop {
        if token.is_cancelled() {
            break;
        }
        run_member(group.clone(), member_id, token.child_token()).await;
        if token.is_cancelled() {
            break;
        }
        // Brief pause before respawn to avoid tight loop on crash
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_member(group: Arc<ConsumerGroup>, member_id: usize, token: CancellationToken) {
    let group_id = format!("{}-{}", group.prefix, group.config.id);
    let topics: Vec<&str> = group.config.topics.iter().map(String::as_str).collect();

    let consumer: StreamConsumer = match group
        .conn
        .to_client_config()
        .set("group.id", &group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "30000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(group_id = %group_id, member_id, "Failed to create consumer: {}", e);
            return;
        }
    };

    if let Err(e) = consumer.subscribe(&topics) {
        warn!(group_id = %group_id, member_id, "Failed to subscribe: {}", e);
        return;
    }

    info!(group_id = %group_id, member_id, "Consumer member started");

    let mut rng = StdRng::from_entropy();
    let mut msg_count: u64 = 0;
    let mut last_sleep = Instant::now();

    loop {
        let behavior = group.behavior.read().await.clone();

        // Intermittent: periodic sleep
        if behavior.behavior_type == BehaviorType::Intermittent {
            if let (Some(sleep_every), Some(sleep_dur)) = (
                behavior.sleep_every_seconds,
                behavior.sleep_duration_seconds,
            ) {
                if last_sleep.elapsed() >= Duration::from_secs(sleep_every) {
                    info!(group_id = %group_id, member_id, "Intermittent sleep for {}s", sleep_dur);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(sleep_dur)) => {}
                        _ = token.cancelled() => break,
                    }
                    last_sleep = Instant::now();
                    continue;
                }
            }
        }

        // Crashing: random crash to trigger rebalance
        if behavior.behavior_type == BehaviorType::Crashing && rng.gen::<f64>() < 0.01 {
            info!(group_id = %group_id, member_id, "Simulated crash → rebalance");
            group.stats.record_rebalance();
            return; // exit this member; outer loop will respawn
        }

        let recv_result = tokio::select! {
            biased;
            _ = token.cancelled() => {
                // Commit what we have before exiting
                break;
            }
            r = consumer.recv() => r,
        };

        match recv_result {
            Err(e) => {
                warn!(group_id = %group_id, member_id, "Recv error: {}", e);
                group.stats.record_error();
            }
            Ok(msg) => {
                let sleep_ms = behavior.processing_ms;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                    _ = token.cancelled() => {
                        let _ = consumer.commit_message(&msg, CommitMode::Sync);
                        break;
                    }
                }

                msg_count += 1;
                group.stats.record_consumed(&group.config.id);

                let commit_every = behavior.commit_every.unwrap_or(1);
                if msg_count % commit_every == 0 {
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        warn!(group_id = %group_id, "Commit error: {}", e);
                    }
                }
            }
        }
    }

    info!(group_id = %group_id, member_id, "Consumer member stopped");
}

async fn run_lag_poller(group: Arc<ConsumerGroup>, token: CancellationToken) {
    let group_id = format!("{}-{}", group.prefix, group.config.id);
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            _ = interval.tick() => {}
        }

        let poller: BaseConsumer = match group
            .conn
            .to_client_config()
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                warn!("Lag poller creation failed for {}: {}", group_id, e);
                continue;
            }
        };

        let timeout = Duration::from_secs(5);
        let mut total_lag: i64 = 0;

        for topic_name in &group.config.topics {
            let metadata = match poller.fetch_metadata(Some(topic_name), timeout) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Lag poller fetch_metadata failed: {}", e);
                    continue;
                }
            };

            for topic_meta in metadata.topics() {
                let mut tpl = TopicPartitionList::new();
                for partition in topic_meta.partitions() {
                    tpl.add_partition(topic_meta.name(), partition.id());
                }

                let committed = match poller.committed_offsets(tpl, timeout) {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("committed_offsets failed: {}", e);
                        continue;
                    }
                };

                for elem in committed.elements() {
                    let high = match poller.fetch_watermarks(
                        elem.topic(),
                        elem.partition(),
                        timeout,
                    ) {
                        Ok((_, high)) => high,
                        Err(_) => continue,
                    };

                    let committed_offset = match elem.offset() {
                        Offset::Offset(n) => n,
                        _ => 0,
                    };

                    total_lag += (high - committed_offset).max(0);
                }
            }
        }

        group.stats.update_lag(&group.config.id, total_lag as u64);
    }
}
