use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{Scenario, ScenarioAction};
use crate::consumer::ConsumerGroup;

pub struct ScenarioRunner {
    pub scenario: Scenario,
    pub groups: Vec<Arc<ConsumerGroup>>,
    pub start_tx: watch::Sender<bool>,
    pub global_token: CancellationToken,
}

impl ScenarioRunner {
    pub async fn run(self, start_time: Instant) {
        info!("Scenario '{}' started", self.scenario.name);

        for step in &self.scenario.steps {
            let target_time = start_time + Duration::from_secs(step.at_seconds);
            let now = Instant::now();
            if target_time > now {
                tokio::time::sleep_until(tokio::time::Instant::from_std(target_time)).await;
            }

            if self.global_token.is_cancelled() {
                break;
            }

            info!(
                scenario = %self.scenario.name,
                at_seconds = step.at_seconds,
                action = ?step.action,
                "Executing step"
            );

            match step.action {
                ScenarioAction::StartAll => {
                    let _ = self.start_tx.send(true);
                }
                ScenarioAction::StopAll => {
                    info!("Scenario stop_all — cancelling global token");
                    self.global_token.cancel();
                    break;
                }
                ScenarioAction::SetBehavior => {
                    if let (Some(target), Some(new_behavior)) =
                        (&step.target, &step.behavior)
                    {
                        if let Some(group) =
                            self.groups.iter().find(|g| &g.config.id == target)
                        {
                            let mut behavior = group.behavior.write().await;
                            *behavior = new_behavior.clone();
                            info!(
                                group_id = %target,
                                behavior_type = ?new_behavior.behavior_type,
                                "Behavior updated"
                            );
                        } else {
                            tracing::warn!("set_behavior: group '{}' not found", target);
                        }
                    }
                }
                ScenarioAction::AddMembers => {
                    if let Some(target) = &step.target {
                        if let Some(group) =
                            self.groups.iter().find(|g| &g.config.id == target)
                        {
                            let count = step.count.unwrap_or(1);
                            // Members added by scenario start immediately (already past start gate)
                            let (_tx, rx) = watch::channel(true);
                            group
                                .clone()
                                .add_members(
                                    count,
                                    self.global_token.clone(),
                                    rx,
                                )
                                .await;
                        } else {
                            tracing::warn!("add_members: group '{}' not found", target);
                        }
                    }
                }
                ScenarioAction::RemoveMembers => {
                    if let Some(target) = &step.target {
                        if let Some(group) =
                            self.groups.iter().find(|g| &g.config.id == target)
                        {
                            let count = step.count.unwrap_or(1);
                            group.remove_members(count).await;
                        } else {
                            tracing::warn!("remove_members: group '{}' not found", target);
                        }
                    }
                }
            }
        }

        info!("Scenario '{}' completed", self.scenario.name);
    }
}
