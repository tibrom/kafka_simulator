mod config;
mod consumer;
mod producer;
mod scenario;
mod stats;
mod template;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::SimConfig;
use crate::consumer::ConsumerGroup;
use crate::scenario::ScenarioRunner;
use crate::stats::Stats;

#[derive(Parser, Debug)]
#[command(name = "kafka-sim", about = "Kafka activity simulator for kfvisor testing")]
struct Cli {
    #[arg(short, long, default_value = "sim.yaml")]
    config: PathBuf,

    #[arg(short, long, help = "Run a named scenario (otherwise: infinite baseline mode)")]
    scenario: Option<String>,

    #[arg(long, help = "Validate config and connection, then exit")]
    dry_run: bool,

    #[arg(long, help = "Write final stats to JSON file")]
    stats_output: Option<PathBuf>,

    #[arg(long, help = "Skip topic creation (expect topics to already exist)")]
    no_create_topics: bool,

    #[arg(long, default_value = "kafka-sim", help = "Consumer group ID prefix")]
    prefix: String,

    #[arg(long, help = "Delete created topics and reset consumer groups on exit")]
    cleanup: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let raw = fs::read_to_string(&cli.config)
        .unwrap_or_else(|e| panic!("Cannot read config {:?}: {}", cli.config, e));
    let sim_config: SimConfig = serde_yaml::from_str(&raw)
        .unwrap_or_else(|e| panic!("Invalid YAML config: {}", e));

    if cli.dry_run {
        return do_dry_run(&sim_config).await;
    }

    let conn = Arc::new(sim_config.connection.clone());

    // Create topics
    if !cli.no_create_topics {
        create_topics(&sim_config, &conn).await?;
    }

    // Fetch partition counts for each producer topic (needed for round_robin key strategy)
    let mut partition_counts: std::collections::HashMap<String, i32> =
        std::collections::HashMap::new();
    {
        let meta_consumer: BaseConsumer = conn.to_client_config().create()?;
        for producer_cfg in &sim_config.producers {
            let meta = meta_consumer
                .fetch_metadata(Some(&producer_cfg.topic), Duration::from_secs(10))
                .unwrap_or_else(|e| {
                    panic!(
                        "Cannot fetch metadata for topic '{}': {}",
                        producer_cfg.topic, e
                    )
                });
            let count = meta
                .topics()
                .first()
                .map(|t| t.partitions().len() as i32)
                .unwrap_or(1);
            partition_counts.insert(producer_cfg.topic.clone(), count);
        }
    }

    let producer_ids: Vec<String> = sim_config.producers.iter().map(|p| p.id.clone()).collect();
    let consumer_ids: Vec<String> = sim_config.consumer_groups.iter().map(|g| g.id.clone()).collect();
    let stats = Stats::new(&producer_ids, &consumer_ids);

    let global_token = CancellationToken::new();
    let (start_tx, start_rx) = watch::channel(false);

    // Build consumer groups
    let groups: Vec<Arc<ConsumerGroup>> = sim_config
        .consumer_groups
        .iter()
        .map(|cfg| {
            Arc::new(ConsumerGroup::new(
                cfg.clone(),
                conn.clone(),
                stats.clone(),
                cli.prefix.clone(),
            ))
        })
        .collect();

    // Spawn consumer members and lag pollers
    let mut lag_handles = Vec::new();
    for group in &groups {
        group
            .clone()
            .spawn_initial_members(global_token.clone(), start_rx.clone())
            .await;
        lag_handles.push(group.clone().spawn_lag_poller(global_token.clone()));
    }

    // Spawn producers
    let mut producer_handles = Vec::new();
    for producer_cfg in sim_config.producers {
        let partition_count = partition_counts.get(&producer_cfg.topic).copied().unwrap_or(1);
        let h = tokio::spawn(crate::producer::run_producer(
            producer_cfg,
            conn.clone(),
            stats.clone(),
            partition_count,
            global_token.clone(),
            start_rx.clone(),
        ));
        producer_handles.push(h);
    }

    // Stats reporter (every 10s)
    let stats_for_reporter = stats.clone();
    let reporter_token = global_token.clone();
    let reporter_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                biased;
                _ = reporter_token.cancelled() => break,
                _ = interval.tick() => stats_for_reporter.print_report(),
            }
        }
    });

    // Scenario or immediate start
    let start_time = Instant::now();
    let mut scenario_handle = None;

    if let Some(scenario_name) = &cli.scenario {
        let scenarios = sim_config.scenarios.unwrap_or_default();
        let scenario = scenarios
            .into_iter()
            .find(|s| &s.name == scenario_name)
            .unwrap_or_else(|| panic!("Scenario '{}' not found in config", scenario_name));

        // Check if scenario has a start_all step; if not, fire start signal now
        let has_start_all = scenario
            .steps
            .iter()
            .any(|s| matches!(s.action, config::ScenarioAction::StartAll));
        if !has_start_all {
            let _ = start_tx.send(true);
        }

        let runner = ScenarioRunner {
            scenario,
            groups: groups.clone(),
            start_tx,
            global_token: global_token.clone(),
        };
        let h = tokio::spawn(async move { runner.run(start_time).await });
        scenario_handle = Some(h);
    } else {
        let _ = start_tx.send(true);
    }

    // Wait for Ctrl+C
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c");
    info!("Ctrl+C received — shutting down gracefully...");
    global_token.cancel();

    // Wait for everything
    for h in producer_handles {
        h.await.ok();
    }
    for group in &groups {
        group.wait_all().await;
    }
    for h in lag_handles {
        h.await.ok();
    }
    if let Some(h) = scenario_handle {
        h.await.ok();
    }
    reporter_handle.await.ok();

    // Final stats
    println!();
    stats.print_report();

    if let Some(output_path) = &cli.stats_output {
        fs::write(output_path, stats.to_json())?;
        info!("Stats written to {:?}", output_path);
    }

    // Cleanup if requested
    if cli.cleanup {
        let admin: AdminClient<DefaultClientContext> = conn.to_client_config().create()?;
        let topic_names: Vec<&str> = sim_config
            .topics
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        let results = admin
            .delete_topics(&topic_names, &AdminOptions::new())
            .await?;
        for result in results {
            match result {
                Ok(name) => info!("Deleted topic: {}", name),
                Err((name, e)) => warn!("Failed to delete topic {}: {:?}", name, e),
            }
        }
    }

    Ok(())
}

async fn do_dry_run(sim_config: &SimConfig) -> anyhow::Result<()> {
    println!("[kafka-sim] dry-run: validating config...");
    println!(
        "  bootstrap_servers: {}",
        sim_config.connection.bootstrap_servers.join(", ")
    );
    println!("  topics ({}):", sim_config.topics.len());
    for t in &sim_config.topics {
        println!(
            "    - {} (partitions={}, rf={})",
            t.name, t.partitions, t.replication_factor
        );
    }
    println!("  producers ({}):", sim_config.producers.len());
    for p in &sim_config.producers {
        println!("    - {} → topic={} rate={}/s", p.id, p.topic, p.rate);
    }
    println!("  consumer_groups ({}):", sim_config.consumer_groups.len());
    for g in &sim_config.consumer_groups {
        println!(
            "    - {} topics={:?} members={}",
            g.id, g.topics, g.members
        );
    }

    // Test Kafka connectivity via a BaseConsumer (sync metadata fetch is fine in async context)
    println!("[kafka-sim] dry-run: testing Kafka connection...");
    let meta_consumer: BaseConsumer = sim_config.connection.to_client_config().create()?;
    match meta_consumer.fetch_metadata(None, Duration::from_secs(10)) {
        Ok(meta) => println!(
            "[kafka-sim] dry-run: connected — {} broker(s) visible",
            meta.brokers().len()
        ),
        Err(e) => {
            eprintln!("[kafka-sim] dry-run: connection failed: {}", e);
            std::process::exit(1);
        }
    }

    println!("[kafka-sim] dry-run: OK");
    Ok(())
}

async fn create_topics(sim_config: &SimConfig, conn: &Arc<config::ConnectionConfig>) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = conn.to_client_config().create()?;

    let new_topics: Vec<NewTopic> = sim_config
        .topics
        .iter()
        .map(|t| {
            let mut nt = NewTopic::new(
                &t.name,
                t.partitions,
                TopicReplication::Fixed(t.replication_factor),
            );
            for (k, v) in &t.config {
                nt = nt.set(k, v);
            }
            nt
        })
        .collect();

    let results = admin.create_topics(&new_topics, &AdminOptions::new()).await?;
    for result in results {
        match result {
            Ok(name) => info!("Created topic: {}", name),
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                info!("Topic already exists (skipping): {}", name)
            }
            Err((name, e)) => warn!("Failed to create topic {}: {:?}", name, e),
        }
    }

    Ok(())
}
