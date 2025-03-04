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

use super::client;
use super::utils::{FixedBodyReader, RequestId, StreamingBodyReader};
use crate::connectors::{prelude::*, utils::mime::MimeCodecMap};
use async_std::channel::{unbounded, Sender};
use either::Either;
use http_types::headers::HeaderValues;
use http_types::Response;
use http_types::{
    headers::{self, HeaderValue},
    mime::BYTE_STREAM,
    Method, Mime, Request,
};
use std::str::FromStr;
use tremor_value::Value;
use value_trait::{Builder, ValueAccess};

/// Body data enum for chunked or non-chunked data
pub(crate) enum BodyData {
    Data(Vec<Vec<u8>>),
    Chunked(Sender<Vec<u8>>),
}

/// Utility for building an HTTP request from a possibly batched event
/// and some configuration values
pub(crate) struct HttpRequestBuilder {
    request_id: RequestId,
    request: Option<Request>,
    body_data: BodyData,
    codec_overwrite: Option<String>,
}

// TODO: do some deduplication with SinkResponse
impl HttpRequestBuilder {
    pub(super) fn new(
        request_id: RequestId,
        meta: Option<&Value>,
        codec_map: &MimeCodecMap,
        config: &client::Config,
        configured_codec: &str,
    ) -> Result<Self> {
        let request_meta = meta.get("request");
        let method = if let Some(method_v) = request_meta.get("method") {
            if let Some(method_str) = method_v.as_str() {
                Method::from_str(method_str)?
            } else {
                return Err("Invalid HTTP Method".into());
            }
        } else {
            config.method
        };
        let url = if let Some(url_v) = request_meta.get("url") {
            if let Some(url_str) = url_v.as_str() {
                Url::parse(url_str)?
            } else {
                return Err("Invalid HTTP URL".into());
            }
        } else {
            config.url.clone()
        };
        let mut request = Request::new(method, url.url().clone());
        let headers = request_meta.get("headers");

        // first insert config headers
        for (config_header_name, config_header_values) in &config.headers {
            match &config_header_values.0 {
                Either::Left(config_header_values) => {
                    for header_value in config_header_values {
                        request.append_header(config_header_name.as_str(), header_value.as_str());
                    }
                }
                Either::Right(header_value) => {
                    request.append_header(config_header_name.as_str(), header_value.as_str());
                }
            }
        }
        // build headers
        if let Some(headers) = headers.as_object() {
            for (name, values) in headers {
                if let Some(header_values) = values.as_array() {
                    let mut v = Vec::with_capacity(header_values.len());
                    for value in header_values {
                        if let Some(header_value) = value.as_str() {
                            v.push(HeaderValue::from_str(header_value)?);
                        }
                    }
                    request.append_header(name.as_ref(), v.as_slice());
                } else if let Some(header_value) = values.as_str() {
                    request.append_header(name.as_ref(), header_value);
                }
            }
        }

        let chunked = request
            .header(headers::TRANSFER_ENCODING)
            .map(HeaderValues::last)
            .map_or(false, |te| te.as_str() == "chunked");

        let header_content_type = request.content_type();

        let codec_overwrite = header_content_type
            .as_ref()
            .and_then(|mime| codec_map.get_codec_name(mime.essence()))
            // only overwrite the codec if it is different from the configured one
            .filter(|codec| *codec != configured_codec)
            .cloned();
        let codec_content_type = codec_overwrite
            .as_ref()
            .and_then(|codec| codec_map.get_mime_type(codec.as_str()))
            .or_else(|| codec_map.get_mime_type(configured_codec))
            .and_then(|mime| Mime::from_str(mime).ok());

        // extract content-type and thus possible codec overwrite only from first element
        // precedence:
        //  1. from headers meta
        //  2. from overwritten codec
        //  3. from configured codec
        //  4. fall back to application/octet-stream if codec doesn't provide a mime-type
        let content_type = Some(
            header_content_type
                .or(codec_content_type)
                .unwrap_or(BYTE_STREAM),
        );

        // set the content type if it is not set yet
        if request.content_type().is_none() {
            if let Some(ct) = content_type {
                request.set_content_type(ct);
            }
        }
        // handle AUTH
        if let Some(auth_header) = config.auth.as_header_value()? {
            request.insert_header(headers::AUTHORIZATION, auth_header);
        }

        let body_data = if chunked {
            let (chunk_tx, chunk_rx) = unbounded();
            let streaming_reader = StreamingBodyReader::new(chunk_rx);
            request.set_body(surf::Body::from_reader(streaming_reader, None));
            // chunked encoding and content-length cannot go together
            request.remove_header(headers::CONTENT_LENGTH);
            BodyData::Chunked(chunk_tx)
        } else {
            BodyData::Data(Vec::with_capacity(4))
        };

        // extract headers
        // determine content-type, override codec and chunked encoding
        Ok(Self {
            request_id,
            request: Some(request),
            body_data,
            codec_overwrite,
        })
    }

    pub(super) async fn append<'event>(
        &mut self,
        value: &'event Value<'event>,
        ingest_ns: u64,
        serializer: &mut EventSerializer,
    ) -> Result<()> {
        let chunks = serializer.serialize_for_stream_with_codec(
            value,
            ingest_ns,
            self.request_id.get(),
            self.codec_overwrite.as_ref(),
        )?;
        self.append_data(chunks).await
    }

    async fn append_data(&mut self, mut chunks: Vec<Vec<u8>>) -> Result<()> {
        match &mut self.body_data {
            BodyData::Chunked(tx) => {
                for chunk in chunks {
                    tx.send(chunk).await?;
                }
            }
            BodyData::Data(data) => data.append(&mut chunks),
        }
        Ok(())
    }

    /// Finalize and send the response.
    /// In the chunked case we have already sent it before.
    ///
    /// After calling this function this instance shouldn't be used anymore
    pub(super) async fn finalize(
        &mut self,
        serializer: &mut EventSerializer,
    ) -> Result<Option<Request>> {
        // finalize the stream
        let rest = serializer.finish_stream(self.request_id.get())?;
        if !rest.is_empty() {
            self.append_data(rest).await?;
        }
        let mut swap = BodyData::Data(vec![]);
        std::mem::swap(&mut swap, &mut self.body_data);
        // send response if necessary
        match swap {
            BodyData::Data(data) => {
                // set body
                let reader = FixedBodyReader::new(data);
                let len = reader.len();
                if let Some(req) = self.request.as_mut() {
                    req.set_body(surf::Body::from_reader(reader, Some(len)));
                }
            }
            BodyData::Chunked(tx) => {
                // signal EOF to the reader
                tx.close();
            }
        }
        Ok(self.request.take())
    }

    /// Return the ready request if it is chunked
    pub(super) fn get_chunked_request(&mut self) -> Option<Request> {
        if matches!(self.body_data, BodyData::Chunked(_)) {
            self.request.take()
        } else {
            None
        }
    }
}

/// Extract request metadata
pub(super) fn extract_request_meta(request: &Request) -> Value<'static> {
    // collect header values into an array for each header
    let headers = request
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                request
                    .header(name)
                    .iter()
                    .flat_map(|value| {
                        let mut a: Vec<Value> = Vec::new();
                        for v in (*value).iter() {
                            a.push(v.as_str().to_string().into());
                        }
                        a.into_iter()
                    })
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    let mut url_meta = Value::object_with_capacity(7);
    let url = request.url();
    url_meta.try_insert("scheme", url.scheme().to_string());
    if !url.username().is_empty() {
        url_meta.try_insert("username", url.username().to_string());
    }
    url.password()
        .and_then(|p| url_meta.try_insert("password", p.to_string()));
    url.host_str()
        .and_then(|h| url_meta.try_insert("host", h.to_string()));
    url.port().and_then(|p| url_meta.try_insert("port", p));
    url_meta.try_insert("path", url.path().to_string());
    url.query()
        .and_then(|q| url_meta.try_insert("query", q.to_string()));
    url.fragment()
        .and_then(|f| url_meta.try_insert("fragment", f.to_string()));

    literal!({
        "method": request.method().to_string(),
        "headers": headers,
        "url_parts": url_meta, // TODO: naming. `url_meta`, `parsed_url`, `url_data` ?
        "url": url.to_string()
    })
}

/// extract response metadata
pub(super) fn extract_response_meta(response: &Response) -> Value<'static> {
    // collect header values into an array for each header
    let headers = response
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                response
                    .header(name)
                    .iter()
                    .flat_map(|value| {
                        let mut a: Vec<Value> = Vec::new();
                        for v in (*value).iter() {
                            a.push(v.as_str().to_string().into());
                        }
                        a.into_iter()
                    })
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    let mut meta = Value::object_with_capacity(3);
    meta.try_insert("status", response.status() as u16);
    meta.try_insert("headers", headers);
    response
        .version()
        .map(|version| meta.try_insert("version", version.to_string()));
    meta
}

#[cfg(test)]
mod test {
    use super::*;
    #[async_std::test]
    async fn builder() -> Result<()> {
        let request_id = RequestId::new(42);
        let meta = None;
        let codec_map = MimeCodecMap::default();
        let c = literal!({"headers": {
            "cake": ["black forst", "cheese"],
            "pie": "key lime"
        }});
        let mut s = EventSerializer::new(
            None,
            CodecReq::Optional("json"),
            vec![],
            &ConnectorType("http".into()),
            &Alias::new("flow", "http"),
        )?;
        let config = client::Config::new(&c)?;
        let configured_codec = "json";

        let mut b =
            HttpRequestBuilder::new(request_id, meta, &codec_map, &config, configured_codec)?;

        let r = b.finalize(&mut s).await?.unwrap();
        assert_eq!(r.header("pie").unwrap().iter().count(), 1);
        assert_eq!(r.header("cake").unwrap().iter().count(), 2);
        Ok(())
    }
}
