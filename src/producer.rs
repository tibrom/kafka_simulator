use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{ConnectionConfig, ProducerConfig};
use crate::stats::Stats;
use crate::template::MessageRenderer;

pub async fn run_producer(
    config: ProducerConfig,
    conn: Arc<ConnectionConfig>,
    stats: Stats,
    partition_count: i32,
    token: CancellationToken,
    mut start_rx: watch::Receiver<bool>,
) {
    let _ = start_rx.wait_for(|v| *v).await;

    let producer: FutureProducer = conn
        .to_client_config()
        .create()
        .expect("Failed to create FutureProducer");

    let mut renderer = MessageRenderer::new(partition_count);
    let base_interval = Duration::from_secs_f64(1.0 / config.rate);
    let mut effective_interval = base_interval;

    let mut burst_active = false;
    let mut burst_until: Option<Instant> = None;
    let mut last_burst_trigger = Instant::now();

    info!(producer_id = %config.id, "Producer started at {}/s", config.rate);

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                info!(producer_id = %config.id, "Producer shutting down");
                break;
            }
            _ = tokio::time::sleep(effective_interval) => {}
        }

        // Update burst state
        if let Some(burst) = &config.burst {
            if burst.enabled {
                let now = Instant::now();
                if burst_active {
                    if let Some(until) = burst_until {
                        if now >= until {
                            burst_active = false;
                            effective_interval = base_interval;
                        }
                    }
                } else if now.duration_since(last_burst_trigger)
                    >= Duration::from_secs(burst.every_seconds)
                {
                    burst_active = true;
                    burst_until = Some(now + Duration::from_secs(burst.duration_seconds));
                    last_burst_trigger = now;
                    let burst_rate = config.rate * burst.multiplier;
                    effective_interval = Duration::from_secs_f64(1.0 / burst_rate);
                }
            }
        }

        let template = config.message.template_or_value();
        let rendered = renderer.render_message(&template);
        let key = renderer.compute_key(
            &config.message.key_strategy,
            config.message.key_field.as_deref(),
            &rendered,
        );

        let record = FutureRecord::to(&config.topic)
            .payload(rendered.as_bytes())
            .key(key.as_str());

        match producer.send(record, Duration::ZERO).await {
            Ok(_) => stats.record_produced(&config.id),
            Err((e, _)) => {
                warn!(producer_id = %config.id, "Send error: {}", e);
                stats.record_error();
            }
        }
    }
}
