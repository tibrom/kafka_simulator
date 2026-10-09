use std::collections::HashMap;
use rdkafka::config::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SimConfig {
    pub connection: ConnectionConfig,
    pub topics: Vec<TopicConfig>,
    pub producers: Vec<ProducerConfig>,
    pub consumer_groups: Vec<ConsumerGroupConfig>,
    pub scenarios: Option<Vec<Scenario>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionConfig {
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub sasl: SaslConfig,
    #[serde(default)]
    pub tls: TlsConfig,
}

impl ConnectionConfig {
    pub fn to_client_config(&self) -> ClientConfig {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", self.bootstrap_servers.join(","));

        let has_sasl = !matches!(self.sasl.mechanism, SaslMechanism::None);
        let security_protocol = match (has_sasl, self.tls.enabled) {
            (false, false) => "PLAINTEXT",
            (false, true) => "SSL",
            (true, false) => "SASL_PLAINTEXT",
            (true, true) => "SASL_SSL",
        };
        cfg.set("security.protocol", security_protocol);

        match self.sasl.mechanism {
            SaslMechanism::None => {}
            SaslMechanism::Plain => {
                cfg.set("sasl.mechanisms", "PLAIN");
                cfg.set("sasl.username", &self.sasl.username);
                cfg.set("sasl.password", &self.sasl.password);
            }
            SaslMechanism::ScramSha256 => {
                cfg.set("sasl.mechanisms", "SCRAM-SHA-256");
                cfg.set("sasl.username", &self.sasl.username);
                cfg.set("sasl.password", &self.sasl.password);
            }
            SaslMechanism::ScramSha512 => {
                cfg.set("sasl.mechanisms", "SCRAM-SHA-512");
                cfg.set("sasl.username", &self.sasl.username);
                cfg.set("sasl.password", &self.sasl.password);
            }
        }

        if self.tls.enabled && !self.tls.ca_cert_path.is_empty() {
            cfg.set("ssl.ca.location", &self.tls.ca_cert_path);
        }

        cfg
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct SaslConfig {
    #[serde(default)]
    pub mechanism: SaslMechanism,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SaslMechanism {
    #[default]
    None,
    Plain,
    ScramSha256,
    ScramSha512,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ca_cert_path: String,
}

#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i32,
    #[serde(default)]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProducerConfig {
    pub id: String,
    pub topic: String,
    pub rate: f64,
    pub burst: Option<BurstConfig>,
    pub message: MessageConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BurstConfig {
    pub enabled: bool,
    pub every_seconds: u64,
    pub multiplier: f64,
    pub duration_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MessageConfig {
    pub format: MessageFormat,
    pub template: Option<String>,
    pub value: Option<String>,
    #[serde(default)]
    pub key_strategy: KeyStrategy,
    pub key_field: Option<String>,
}

impl MessageConfig {
    pub fn template_or_value(&self) -> String {
        self.template
            .clone()
            .or_else(|| self.value.clone())
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum MessageFormat {
    #[default]
    Json,
    String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyStrategy {
    #[default]
    None,
    Random,
    Field,
    RoundRobin,
    Skewed,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConsumerGroupConfig {
    pub id: String,
    pub topics: Vec<String>,
    pub members: usize,
    pub behavior: BehaviorConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BehaviorConfig {
    #[serde(rename = "type")]
    pub behavior_type: BehaviorType,
    pub processing_ms: u64,
    pub commit_every: Option<u64>,
    pub sleep_every_seconds: Option<u64>,
    pub sleep_duration_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BehaviorType {
    Normal,
    Slow,
    Lagging,
    Intermittent,
    Crashing,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Scenario {
    pub name: String,
    pub steps: Vec<ScenarioStep>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScenarioStep {
    pub at_seconds: u64,
    pub action: ScenarioAction,
    pub target: Option<String>,
    pub behavior: Option<BehaviorConfig>,
    pub count: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioAction {
    StartAll,
    StopAll,
    SetBehavior,
    AddMembers,
    RemoveMembers,
}
