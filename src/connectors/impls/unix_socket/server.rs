// Copyright 2022, The Tremor Team
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

//! Unix socket server
//!
//! Identifies client connections by their stream id, a u64.
//! There is no other data we can associate to a connection.
//!
//! When we have metadata like on an event we receive via the sink part of this connector:
//!
//! ```json
//! {
//!     "unix_socket_server": {
//!         "peer": 123
//!     }
//! }
//! ```
//!
//! We try to route the event to the connection with `stream_id` `123`.
use crate::connectors::prelude::*;
use crate::connectors::sink::channel_sink::ChannelSinkMsg;
use async_std::os::unix::net::UnixListener;
use async_std::path::PathBuf;
use async_std::task::JoinHandle;
use async_std::{
    channel::{bounded, Receiver, Sender},
    prelude::FutureExt,
};

use super::{UnixSocketReader, UnixSocketWriter};

const URL_SCHEME: &str = "tremor-unix-socket-server";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub path: String,
    pub permissions: Option<String>,
    /// receive buffer size
    #[serde(default = "default_buf_size")]
    buf_size: usize,
}

impl ConfigImpl for Config {}

//struct ConnectionMeta {}

#[derive(Debug, Default)]
pub(crate) struct Builder {}
#[async_trait::async_trait()]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "unix_socket_server".into()
    }

    async fn build_cfg(
        &self,
        _: &Alias,
        _: &ConnectorConfig,
        config: &Value,
        _kill_switch: &KillSwitch,
    ) -> Result<Box<dyn Connector>> {
        let config = Config::new(config)?;
        let (sink_tx, sink_rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        Ok(Box::new(UnixSocketServer {
            config,
            sink_tx,
            sink_rx,
        }))
    }
}

/// just a `stream_id`
#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
struct ConnectionMeta(u64);

///
/// Expect connection meta as:
///
/// ```json
/// {
///     "unix_socket_server": {
///         "peer": 123
///     }
/// }
fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    meta.get_u64("peer").map(ConnectionMeta)
}

struct UnixSocketServer {
    config: Config,
    sink_tx: Sender<ChannelSinkMsg<ConnectionMeta>>,
    sink_rx: Receiver<ChannelSinkMsg<ConnectionMeta>>,
}

#[async_trait::async_trait()]
impl Connector for UnixSocketServer {
    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }

    async fn create_source(
        &mut self,
        source_context: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let sink_runtime = ChannelSinkRuntime::new(self.sink_tx.clone());
        let source = UnixSocketSource::new(self.config.clone(), sink_runtime);
        builder.spawn(source, source_context).map(Some)
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ChannelSink::from_channel_no_meta(
            resolve_connection_meta,
            builder.reply_tx(),
            self.sink_tx.clone(),
            self.sink_rx.clone(),
        );
        builder.spawn(sink, ctx).map(Some)
    }
}

struct UnixSocketSource {
    config: Config,
    listener_task: Option<JoinHandle<()>>,
    connection_rx: Receiver<SourceReply>,
    runtime: ChannelSourceRuntime,
    sink_runtime: ChannelSinkRuntime<ConnectionMeta>,
}

impl UnixSocketSource {
    fn new(config: Config, sink_runtime: ChannelSinkRuntime<ConnectionMeta>) -> Self {
        let (tx, rx) = bounded(crate::QSIZE.load(Ordering::Relaxed));
        let runtime = ChannelSourceRuntime::new(tx);
        Self {
            config,
            listener_task: None,
            connection_rx: rx,
            runtime,
            sink_runtime,
        }
    }
}

#[async_trait::async_trait()]
impl Source for UnixSocketSource {
    async fn connect(&mut self, ctx: &SourceContext, _attempt: &Attempt) -> Result<bool> {
        if let Some(listener_task) = self.listener_task.take() {
            listener_task.cancel().await;
        }
        let path = PathBuf::from(&self.config.path);
        if path.exists().await {
            async_std::fs::remove_file(&path).await?;
        }
        let listener = UnixListener::bind(&path).await?;
        if let Some(mode_description) = self.config.permissions.as_ref() {
            let mut mode = file_mode::Mode::empty();
            mode.set_str_umask(mode_description, 0)?;
            mode.set_mode_path(&path)?;
        }
        let buf_size = self.config.buf_size;
        let ctx = ctx.clone();
        let runtime = self.runtime.clone();
        let sink_runtime = self.sink_runtime.clone();
        self.listener_task = Some(spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();
            let origin_uri = EventOriginUri {
                scheme: URL_SCHEME.to_string(),
                host: hostname(),
                port: None,
                path: vec![path.display().to_string()],
            };
            while ctx.quiescence_beacon().continue_reading().await {
                match listener.accept().timeout(ACCEPT_TIMEOUT).await {
                    Ok(Ok((stream, _peer_addr))) => {
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta = ConnectionMeta(stream_id);

                        /*
                            {
                                "unix_socket_server": {
                                    "peer": 123
                                }
                            }

                            let $unix_socket_server = { "peer": 123 };
                        */
                        let meta = ctx.meta(literal!({ "peer": stream_id }));
                        let reader = UnixSocketReader::new(
                            stream.clone(),
                            vec![0; buf_size],
                            ctx.alias().to_string(),
                            origin_uri.clone(),
                            meta,
                        );
                        sink_runtime
                            .register_stream_writer(
                                stream_id,
                                Some(connection_meta),
                                &ctx,
                                UnixSocketWriter::new(stream),
                            )
                            .await;
                        runtime.register_stream_reader(stream_id, &ctx, reader);
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => continue,
                };
            }
            Ok(())
        }));
        Ok(true)
    }
    async fn pull_data(&mut self, _pull_id: &mut u64, _ctx: &SourceContext) -> Result<SourceReply> {
        Ok(self.connection_rx.recv().await?)
    }

    async fn on_stop(&mut self, _ctx: &SourceContext) -> Result<()> {
        if let Some(listener_task) = self.listener_task.take() {
            // stop acceptin' new connections
            listener_task.cancel().await;
        }
        Ok(())
    }

    fn is_transactional(&self) -> bool {
        false
    }

    fn asynchronous(&self) -> bool {
        true
    }
}
