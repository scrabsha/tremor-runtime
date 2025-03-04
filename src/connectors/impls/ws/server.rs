// Copyright 2021, The Tremor Team
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

use super::{WsReader, WsWriter};
use crate::connectors::utils::tls::{load_server_config, TLSServerConfig};
use crate::connectors::{prelude::*, utils::ConnectionMeta};
use async_std::task::JoinHandle;
use async_std::{net::TcpListener, prelude::FutureExt};
use async_tls::TlsAcceptor;
use async_tungstenite::accept_async;
use futures::StreamExt;
use rustls::ServerConfig;
use simd_json::ValueAccess;
use std::net::SocketAddr;
use std::sync::Arc;

const URL_SCHEME: &str = "tremor-ws-server";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    // kept as a str, so it is re-resolved upon each connect
    url: Url<super::WsDefaults>,
    tls: Option<TLSServerConfig>,
}

impl ConfigImpl for Config {}

#[allow(clippy::module_name_repetitions)]
pub(crate) struct WsServer {
    config: Config,
    accept_task: Option<JoinHandle<()>>,
    sink_runtime: Option<ChannelSinkRuntime<ConnectionMeta>>,
    source_runtime: Option<ChannelSourceRuntime>,
    tls_server_config: Option<ServerConfig>,
}

#[derive(Debug, Default)]
pub(crate) struct Builder {}

#[async_trait::async_trait]
impl ConnectorBuilder for Builder {
    fn connector_type(&self) -> ConnectorType {
        "ws_server".into()
    }
    async fn build_cfg(
        &self,
        _id: &Alias,
        _: &ConnectorConfig,
        raw_config: &Value,
        _kill_switch: &KillSwitch,
    ) -> crate::errors::Result<Box<dyn Connector>> {
        let config = Config::new(raw_config)?;

        let tls_server_config = if let Some(tls_config) = config.tls.as_ref() {
            Some(load_server_config(tls_config)?)
        } else {
            None
        };

        Ok(Box::new(WsServer {
            config,
            accept_task: None,  // not yet started
            sink_runtime: None, // replaced in create_sink()
            source_runtime: None,
            tls_server_config,
        }))
    }
}

fn resolve_connection_meta(meta: &Value) -> Option<ConnectionMeta> {
    let peer = meta.get("peer");
    peer.get_u16("port")
        .zip(peer.get_str("host"))
        .map(|(port, host)| -> ConnectionMeta {
            ConnectionMeta {
                host: host.to_string(),
                port,
            }
        })
}

impl WsServer {
    fn meta(peer: SocketAddr, has_tls: bool) -> Value<'static> {
        let peer_ip = peer.ip().to_string();
        let peer_port = peer.port();

        literal!({
            "tls": has_tls,
            "peer": {
                "host": peer_ip,
                "port": peer_port
            }
        })
    }
}

#[async_trait::async_trait()]
impl Connector for WsServer {
    async fn on_stop(&mut self, _ctx: &ConnectorContext) -> Result<()> {
        if let Some(accept_task) = self.accept_task.take() {
            // stop acceptin' new connections
            accept_task.cancel().await;
        }
        Ok(())
    }

    async fn create_source(
        &mut self,
        ctx: SourceContext,
        builder: SourceManagerBuilder,
    ) -> Result<Option<SourceAddr>> {
        let source = ChannelSource::new(builder.qsize());
        self.source_runtime = Some(source.runtime());
        let addr = builder.spawn(source, ctx)?;

        Ok(Some(addr))
    }

    async fn create_sink(
        &mut self,
        ctx: SinkContext,
        builder: SinkManagerBuilder,
    ) -> Result<Option<SinkAddr>> {
        let sink = ChannelSink::new_with_meta(
            builder.qsize(),
            resolve_connection_meta,
            builder.reply_tx(),
        );

        self.sink_runtime = Some(sink.runtime());
        let addr = builder.spawn(sink, ctx)?;
        Ok(Some(addr))
    }

    #[allow(clippy::too_many_lines)]
    async fn connect(&mut self, ctx: &ConnectorContext, _attempt: &Attempt) -> Result<bool> {
        // TODO: this can be simplified as the connect can be moved into the source
        let path = vec![self.config.url.port_or_dflt().to_string()];

        let source_runtime = self
            .source_runtime
            .clone()
            .ok_or("Source runtime not initialized")?;
        let sink_runtime = self
            .sink_runtime
            .clone()
            .ok_or("sink runtime not initialized")?;

        // cancel last accept task if necessary, this will drop the previous listener
        if let Some(previous_handle) = self.accept_task.take() {
            previous_handle.cancel().await;
        }

        // TODO: allow for other sockets
        let host = self.config.url.host_or_local();
        let port = self
            .config
            .url
            .port()
            .unwrap_or(if self.config.url.scheme() == "wss" {
                443
            } else {
                80
            });
        let listener = TcpListener::bind((host, port)).await?;

        let ctx = ctx.clone();
        let tls_server_config = self.tls_server_config.clone();

        // accept task
        self.accept_task = Some(spawn_task(ctx.clone(), async move {
            let mut stream_id_gen = StreamIdGen::default();
            while ctx.quiescence_beacon.continue_reading().await {
                match listener.accept().timeout(ACCEPT_TIMEOUT).await {
                    Ok(Ok((tcp_stream, peer_addr))) => {
                        let stream_id: u64 = stream_id_gen.next_stream_id();
                        let connection_meta: ConnectionMeta = peer_addr.into();

                        // Async<T> allows us to read in one thread and write in another concurrently - see its documentation
                        // So we don't need no BiLock like we would when using `.split()`
                        let origin_uri = EventOriginUri {
                            scheme: URL_SCHEME.to_string(),
                            host: peer_addr.ip().to_string(),
                            port: Some(peer_addr.port()),
                            path: path.clone(), // captures server port
                        };

                        let tls_acceptor: Option<TlsAcceptor> = tls_server_config
                            .clone()
                            .map(|sc| TlsAcceptor::from(Arc::new(sc)));
                        if let Some(acceptor) = tls_acceptor {
                            let meta = ctx.meta(WsServer::meta(peer_addr, true));
                            // TODO: this should live in its own task, as it requires rome roundtrips :()
                            let tls_stream = acceptor.accept(tcp_stream).await?;
                            let ws_stream = accept_async(tls_stream).await?;
                            debug!("{ctx} new connection from {peer_addr}");

                            let (ws_write, ws_read) = ws_stream.split();

                            let ws_writer = WsWriter::new_tls_server(ws_write);
                            sink_runtime
                                .register_stream_writer(
                                    stream_id,
                                    Some(connection_meta.clone()),
                                    &ctx,
                                    ws_writer,
                                )
                                .await;

                            let ws_reader = WsReader::new(
                                ws_read,
                                sink_runtime.clone(),
                                origin_uri.clone(),
                                meta,
                                ctx.clone(),
                            );
                            source_runtime.register_stream_reader(stream_id, &ctx, ws_reader);
                        } else {
                            let ws_stream = match accept_async(tcp_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("{ctx} Websocket connection error: {e}");
                                    continue;
                                }
                            };
                            debug!("{ctx} new connection from {peer_addr}",);

                            let (ws_write, ws_read) = ws_stream.split();

                            let meta = ctx.meta(WsServer::meta(peer_addr, false));

                            let ws_writer = WsWriter::new(ws_write);

                            sink_runtime
                                .register_stream_writer(
                                    stream_id,
                                    Some(connection_meta.clone()),
                                    &ctx,
                                    ws_writer,
                                )
                                .await;

                            let ws_reader = WsReader::new(
                                ws_read,
                                sink_runtime.clone(),
                                origin_uri.clone(),
                                meta,
                                ctx.clone(),
                            );
                            source_runtime.register_stream_reader(stream_id, &ctx, ws_reader);
                        }
                    }
                    Ok(Err(e)) => return Err(e.into()),
                    Err(_) => continue,
                };
            }
            Ok(())
        }));

        Ok(true)
    }

    fn codec_requirements(&self) -> CodecReq {
        CodecReq::Required
    }
}
