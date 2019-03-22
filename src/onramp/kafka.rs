// Copyright 2018-2019, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::dflt;
use crate::errors::*;
use crate::onramp::prelude::*;

//NOTE: This is required for StreamHander's stream
use futures::stream::Stream;
use hashbrown::HashMap;
use hostname::get_hostname;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::Message;
use rdkafka_sys;
use serde_yaml::Value;
use std::thread;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// kafka group ID to register with
    pub group_id: String,
    /// List of topics to subscribe to
    pub topics: Vec<String>,
    /// List of bootstrap brokers
    pub brokers: Vec<String>,
    /// If sync is set to true the kafka onramp will wait for an event
    /// to be fully acknowledged before fetching the next one. Defaults
    /// to `false`. Do not use in combination with batching offramps!
    #[serde(default = "dflt::d_false")]
    pub sync: bool,
    /// Optional rdkafka configuration
    ///
    /// Default settings:
    /// * `client.id` - `"tremor-<hostname>-<thread id>"`
    /// * `bootstrap.servers` - `brokers` from the config concatinated by `,`
    /// * `enable.partition.eof` - `"false"`
    /// * `session.timeout.ms` - `"6000"`
    /// * `enable.auto.commit` - `"true"`
    /// * `auto.commit.interval.ms"` - `"5000"`
    /// * `enable.auto.offset.store` - `"true"`
    pub rdkafka_options: Option<HashMap<String, String>>,
}

pub struct Kafka {
    pub config: Config,
}

impl OnrampImpl for Kafka {
    fn from_config(config: &Option<Value>) -> Result<Box<Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Kafka { config }))
        } else {
            Err("Missing config for blaster onramp".into())
        }
    }
}

// A simple context to customize the consumer behavior and print a log line every time
// offsets are committed
pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(
        &self,
        result: KafkaResult<()>,
        _offsets: *mut rdkafka_sys::RDKafkaTopicPartitionList,
    ) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn onramp_loop(rx: Receiver<OnrampMsg>, config: Config, codec: String) -> Result<()> {
    let codec = codec::lookup(&codec)?;
    let hostname = match get_hostname() {
        Some(h) => h,
        None => "tremor-host.local".to_string(),
    };
    let context = LoggingConsumerContext;
    let tid = 0; //TODO: get a good thread id
    let mut client_config = ClientConfig::new();
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    info!("Starting kafka onramp");
    let client_config = client_config
        .set("group.id", &config.group_id)
        .set("client.id", &format!("tremor-{}-{}", hostname, tid))
        .set("bootstrap.servers", &config.brokers.join(","))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "true")
        .set_log_level(RDKafkaLogLevel::Debug);

    let client_config = if let Some(options) = config.rdkafka_options.clone() {
        options
            .iter()
            .fold(client_config, |c: &mut ClientConfig, (k, v)| c.set(k, v))
    } else {
        client_config
    };

    let client_config = client_config.to_owned();
    let consumer: LoggingConsumer = client_config
        .create_with_context(context)
        .expect("Consumer creation failed");

    let topics: Vec<&str> = config.topics.iter().map(|topic| topic.as_str()).collect();

    let stream = consumer.start();

    info!("[kafka] subscribing to: {:?}", topics);
    // This is terribly ugly, thank you rdkafka!
    // We need to do this because:
    // - subscribing to a topic that does not exist will brick the whole consumer
    // - subscribing to a topic that does not exist will claim to succeed
    // - getting the metadata of a topic that does not exist will claim to succeed
    // - The only indication of it missing is in the metadata, in the topics list
    //   in the errors ...
    //
    // This is terrible :/
    let mut id = 0;
    for topic in topics {
        match consumer.fetch_metadata(Some(topic), Duration::from_secs(1)) {
            Ok(m) => {
                let errors: Vec<_> = m.topics().iter().map(|t| t.error()).collect();
                match errors.as_slice() {
                    [None] => match consumer.subscribe(&[topic]) {
                        Ok(()) => info!("Subscribed to topic: {}", topic),
                        Err(e) => error!("Kafka error for topic '{}': {}", topic, e),
                    },
                    [Some(e)] => {
                        error!("Kafka error for topic '{}': {:?}. Not subscring!", topic, e)
                    }
                    _ => error!("Unknown kafka error for topic '{}'. Not subscring!", topic),
                }
            }
            Err(e) => error!("Kafka error for topic '{}': {}. Not subscring!", topic, e),
        };
    }

    // We do this twice so we don't consume a message from kafka and then wait
    // as this could lead to timeouts
    while pipelines.is_empty() {
        match rx.recv()? {
            OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
            OnrampMsg::Disconnect { tx, .. } => {
                let _ = tx.send(true);
                return Ok(());
            }
        };
    }
    for m in stream.wait() {
        while pipelines.is_empty() {
            match rx.recv()? {
                OnrampMsg::Connect(mut ps) => pipelines.append(&mut ps),
                OnrampMsg::Disconnect { tx, .. } => {
                    let _ = tx.send(true);
                    return Ok(());
                }
            };
        }
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(_e) => return Err("Crossbream receive error".into()),
            Ok(OnrampMsg::Connect(mut ps)) => pipelines.append(&mut ps),
            Ok(OnrampMsg::Disconnect { id, tx }) => {
                pipelines.retain(|(pipeline, _)| pipeline != &id);
                if pipelines.is_empty() {
                    let _ = tx.send(true);
                    break;
                } else {
                    let _ = tx.send(false);
                }
            }
        };

        if let Ok(m) = m {
            if let Ok(m) = m {
                if let Some(data) = m.payload_view::<[u8]>() {
                    if let Ok(data) = data {
                        id += 1;
                        send_event(&pipelines, &codec, id, EventValue::Raw(data.to_vec()));
                    } else {
                        error!("failed to fetch data from kafka")
                    }
                } else {
                    error!("No data in kafka message");
                }
            } else {
                error!("Failed to fetch kafka message.");
            }
        }
    }
    Ok(())
}

impl Onramp for Kafka {
    fn start(&mut self, codec: String) -> Result<OnrampAddr> {
        let (tx, rx) = bounded(0);
        let config = self.config.clone();
        //        let id = self.id.clone();
        thread::Builder::new()
            .name(format!("onramp-kafka-{}", "???"))
            .spawn(move || {
                if let Err(e) = onramp_loop(rx, config, codec) {
                    error!("[Onramp] Error: {}", e)
                }
            })?;
        Ok(tx)
    }
    fn default_codec(&self) -> &str {
        "json"
    }
}
