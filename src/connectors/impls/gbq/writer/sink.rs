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

use crate::connectors::google::AuthInterceptor;
use crate::connectors::impls::gbq::writer::Config;
use crate::connectors::prelude::*;
use async_std::prelude::{FutureExt, StreamExt};
use futures::stream;
use googapis::google::cloud::bigquery::storage::v1::append_rows_request::ProtoData;
use googapis::google::cloud::bigquery::storage::v1::big_query_write_client::BigQueryWriteClient;
use googapis::google::cloud::bigquery::storage::v1::table_field_schema::Type as TableType;
use googapis::google::cloud::bigquery::storage::v1::{
    append_rows_request, table_field_schema, write_stream, AppendRowsRequest,
    CreateWriteStreamRequest, ProtoRows, ProtoSchema, TableFieldSchema, WriteStream,
};
use gouth::Token;
use prost::encoding::WireType;
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use std::collections::HashMap;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Status;

pub(crate) struct GbqSink {
    client: Option<BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>>,
    write_stream: Option<WriteStream>,
    mapping: Option<JsonToProtobufMapping>,
    config: Config,
}

struct Field {
    table_type: TableType,
    tag: u32,

    // ignored if the table_type is not struct
    subfields: HashMap<String, Field>,
}

struct JsonToProtobufMapping {
    fields: HashMap<String, Field>,
    descriptor: DescriptorProto,
}

fn map_field(
    schema_name: &str,
    raw_fields: &Vec<TableFieldSchema>,
    ctx: &SinkContext,
) -> (DescriptorProto, HashMap<String, Field>) {
    // The capacity for nested_types isn't known here, as it depends on the number of fields that have the struct type
    let mut nested_types = vec![];
    let mut proto_fields = Vec::with_capacity(raw_fields.len());
    let mut fields = HashMap::with_capacity(raw_fields.len());
    let mut tag: u16 = 1;

    for raw_field in raw_fields {
        let mut type_name = None;
        let mut subfields = HashMap::with_capacity(raw_field.fields.len());

        let table_type =
            if let Some(table_type) = table_field_schema::Type::from_i32(raw_field.r#type) {
                table_type
            } else {
                warn!("Found a field of unknown type: {}", raw_field.name);

                continue;
            };

        let grpc_type = match table_type {
            TableType::Int64 => field_descriptor_proto::Type::Int64,
            TableType::Double => field_descriptor_proto::Type::Double,
            TableType::Bool => field_descriptor_proto::Type::Bool,
            TableType::Bytes => field_descriptor_proto::Type::Bytes,


            TableType::String
            // YYYY-[M]M-[D]D
            | TableType::Date
            // [H]H:[M]M:[S]S[.DDDDDD|.F]
            | TableType::Time
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]]
            | TableType::Datetime
            // The GEOGRAPHY type is based on the OGC Simple Features specification (SFS)
            | TableType::Geography
            // String, because it has decimal precision, f32/f64 would lose precision
            | TableType::Numeric
            | TableType::Bignumeric
            // [sign]Y-M [sign]D [sign]H:M:S[.F]
            | TableType::Interval
            | TableType::Json
            // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.F]][time zone]
            | TableType::Timestamp => field_descriptor_proto::Type::String,
            TableType::Struct => {
                let type_name_for_field = format!("struct_{}", raw_field.name);
                let mapped = map_field(&type_name_for_field, &raw_field.fields, ctx);
                nested_types.push(mapped.0);
                subfields = mapped.1;

                type_name = Some(type_name_for_field);
                field_descriptor_proto::Type::Message
            }

            TableType::Unspecified => {
                warn!("{} Found a field of unspecified type: {}", ctx, raw_field.name);
                continue;
            }
        };

        proto_fields.push(FieldDescriptorProto {
            name: Some(raw_field.name.to_string()),
            number: Some(i32::from(tag)),
            label: None,
            r#type: Some(i32::from(grpc_type)),
            type_name,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });

        fields.insert(
            raw_field.name.to_string(),
            Field {
                table_type,
                tag: u32::from(tag),
                subfields,
            },
        );

        tag += 1;
    }

    (
        DescriptorProto {
            name: Some(schema_name.to_string()),
            field: proto_fields,
            extension: vec![],
            nested_type: nested_types,
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        },
        fields,
    )
}

fn encode_field(val: &Value, field: &Field, result: &mut Vec<u8>) -> Result<()> {
    let tag = field.tag;

    // fixme check which fields are required and fail if they're missing
    // fixme do not panic if the tremor type does not match
    match field.table_type {
        TableType::Double => prost::encoding::double::encode(
            tag,
            &val.as_f64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("f64", val.value_type()))?,
            result,
        ),
        TableType::Int64 => prost::encoding::int64::encode(
            tag,
            &val.as_i64()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("i64", val.value_type()))?,
            result,
        ),
        TableType::Bool => prost::encoding::bool::encode(
            tag,
            &val.as_bool()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("bool", val.value_type()))?,
            result,
        ),
        TableType::String
        | TableType::Date
        | TableType::Time
        | TableType::Datetime
        | TableType::Timestamp
        // String, because it has decimal precision, f32/f64 would lose precision
        | TableType::Numeric
        | TableType::Bignumeric
        | TableType::Geography => {
            prost::encoding::string::encode(
                tag,
                &val.as_str()
                    .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("string", val.value_type()))?
                    .to_string(),
                result,
            );
        }
        TableType::Struct => {
            let mut struct_buf: Vec<u8> = vec![];
            for (k, v) in val
                .as_object()
                .ok_or_else(|| ErrorKind::BigQueryTypeMismatch("object", val.value_type()))?
            {
                let subfield_description = field.subfields.get(&k.to_string());

                if let Some(subfield_description) = subfield_description {
                    encode_field(v, subfield_description, &mut struct_buf)?;
                } else {
                    warn!(
                        "Passed field {} as struct field, not present in definition",
                        k
                    );
                }
            }
            prost::encoding::encode_key(tag, WireType::LengthDelimited, result);
            prost::encoding::encode_varint(struct_buf.len() as u64, result);
            result.append(&mut struct_buf);
        }
        TableType::Bytes => {
            prost::encoding::bytes::encode(
                tag,
                &Vec::from(
                    val.as_bytes().ok_or_else(|| {
                        ErrorKind::BigQueryTypeMismatch("bytes", val.value_type())
                    })?,
                ),
                result,
            );
        }
        TableType::Json => {
            warn!("Found a field of type JSON, this is not supported, ignoring.");
        }
        TableType::Interval => {
            warn!("Found a field of type Interval, this is not supported, ignoring.");
        }

        TableType::Unspecified => {
            warn!("Found a field of unspecified type - ignoring.");
        }
    }

    Ok(())
}

impl JsonToProtobufMapping {
    pub fn new(vec: &Vec<TableFieldSchema>, ctx: &SinkContext) -> Self {
        let descriptor = map_field("table", vec, ctx);

        Self {
            descriptor: descriptor.0,
            fields: descriptor.1,
        }
    }

    pub fn map(&self, value: &Value) -> Result<Vec<u8>> {
        if let Some(obj) = value.as_object() {
            let mut result = Vec::with_capacity(obj.len());

            for (key, val) in obj {
                if let Some(field) = self.fields.get(&key.to_string()) {
                    encode_field(val, field, &mut result)?;
                }
            }

            return Ok(result);
        }

        Err(ErrorKind::BigQueryTypeMismatch("object", value.value_type()).into())
    }

    pub fn descriptor(&self) -> &DescriptorProto {
        &self.descriptor
    }
}
impl GbqSink {
    pub fn new(config: Config) -> Self {
        Self {
            client: None,
            write_stream: None,
            mapping: None,
            config,
        }
    }

    #[cfg(test)]
    pub fn set_client(
        &mut self,
        client: BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>,
    ) {
        self.client = Some(client);
    }
}

#[async_trait::async_trait]
impl Sink for GbqSink {
    async fn on_event(
        &mut self,
        _input: &str,
        event: Event,
        ctx: &SinkContext,
        _serializer: &mut EventSerializer,
        _start: u64,
    ) -> Result<SinkReply> {
        let client = self.client.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "BigQuery",
            "The client is not connected",
        ))?;
        let write_stream = self
            .write_stream
            .as_ref()
            .ok_or(ErrorKind::ClientNotAvailable(
                "BigQuery",
                "The write stream is not available",
            ))?;
        let mapping = self.mapping.as_mut().ok_or(ErrorKind::ClientNotAvailable(
            "BigQuery",
            "The mapping is not available",
        ))?;

        let mut serialized_rows = Vec::with_capacity(event.len());

        for data in event.value_iter() {
            serialized_rows.push(mapping.map(data)?);
        }

        let request = AppendRowsRequest {
            write_stream: write_stream.name.clone(),
            offset: None,
            trace_id: "".to_string(),
            rows: Some(append_rows_request::Rows::ProtoRows(ProtoData {
                writer_schema: Some(ProtoSchema {
                    proto_descriptor: Some(mapping.descriptor().clone()),
                }),
                rows: Some(ProtoRows { serialized_rows }),
            })),
        };

        let append_response = client
            .append_rows(stream::iter(vec![request]))
            .timeout(Duration::from_nanos(self.config.request_timeout))
            .await;

        let append_response = if let Ok(append_response) = append_response {
            append_response
        } else {
            ctx.notifier.connection_lost().await?;

            return Ok(SinkReply::FAIL);
        };

        if let Ok(x) = append_response?
            .into_inner()
            .next()
            .timeout(Duration::from_nanos(self.config.request_timeout))
            .await
        {
            match x {
                Some(Ok(_)) => Ok(SinkReply::ACK),
                Some(Err(e)) => {
                    error!("BigQuery error: {}", e);

                    Ok(SinkReply::FAIL)
                }
                None => Ok(SinkReply::NONE),
            }
        } else {
            ctx.notifier.connection_lost().await?;

            Ok(SinkReply::FAIL)
        }
    }

    async fn connect(&mut self, ctx: &SinkContext, _attempt: &Attempt) -> Result<bool> {
        info!("{ctx} Connecting to BigQuery");
        let token = Token::new()?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(googapis::CERTIFICATES))
            .domain_name("bigquerystorage.googleapis.com");

        let channel = Channel::from_static("https://bigquerystorage.googleapis.com")
            .connect_timeout(Duration::from_nanos(self.config.connect_timeout))
            .tls_config(tls_config)?
            .connect()
            .await?;

        let interceptor_ctx = ctx.clone();
        let mut client = BigQueryWriteClient::with_interceptor(
            channel,
            AuthInterceptor {
                token: Box::new(move || match token.header_value() {
                    Ok(val) => Ok(val),
                    Err(e) => {
                        error!("{interceptor_ctx} Failed to get token for BigQuery: {}", e);

                        Err(Status::unavailable(
                            "Failed to retrieve authentication token.",
                        ))
                    }
                }),
            },
        );

        let write_stream = client
            .create_write_stream(CreateWriteStreamRequest {
                parent: self.config.table_id.clone(),
                write_stream: Some(WriteStream {
                    // The stream name here will be ignored and a generated value will be set in the response
                    name: "".to_string(),
                    r#type: i32::from(write_stream::Type::Committed),
                    create_time: None,
                    commit_time: None,
                    table_schema: None,
                }),
            })
            .await?
            .into_inner();

        let mapping = JsonToProtobufMapping::new(
            &write_stream
                .table_schema
                .as_ref()
                .ok_or(ErrorKind::GbqSinkFailed("Table schema was not provided"))?
                .clone()
                .fields,
            ctx,
        );

        self.mapping = Some(mapping);
        self.write_stream = Some(write_stream);
        self.client = Some(client);

        Ok(true)
    }

    fn auto_ack(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connectors::impls::gbq;
    use crate::connectors::reconnect::ConnectionLostNotifier;
    use crate::connectors::tests::ConnectorHarness;
    use googapis::google::cloud::bigquery::storage::v1::table_field_schema::Mode;
    use std::sync::Arc;
    use value_trait::StaticNode;

    #[test]
    fn skips_unknown_field_types() {
        let (rx, _tx) = async_std::channel::unbounded();

        let result = map_field(
            "name",
            &vec![TableFieldSchema {
                name: "something".to_string(),
                r#type: -1,
                mode: Mode::Required.into(),
                fields: vec![],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &SinkContext {
                uid: Default::default(),
                alias: Alias::new("flow", "connector"),
                connector_type: Default::default(),
                quiescence_beacon: Default::default(),
                notifier: ConnectionLostNotifier::new(rx),
            },
        );

        assert_eq!(result.0.field.len(), 0);
        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn skips_fields_of_unspecified_type() {
        let (rx, _tx) = async_std::channel::unbounded();

        let result = map_field(
            "name",
            &vec![TableFieldSchema {
                name: "something".to_string(),
                r#type: TableType::Unspecified.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &SinkContext {
                uid: Default::default(),
                alias: Alias::new("flow", "connector"),
                connector_type: Default::default(),
                quiescence_beacon: Default::default(),
                notifier: ConnectionLostNotifier::new(rx),
            },
        );

        assert_eq!(result.0.field.len(), 0);
        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn can_map_simple_field() {
        let data = vec![
            (TableType::Int64, field_descriptor_proto::Type::Int64),
            (TableType::Double, field_descriptor_proto::Type::Double),
            (TableType::Bool, field_descriptor_proto::Type::Bool),
            (TableType::Bytes, field_descriptor_proto::Type::Bytes),
            (TableType::Timestamp, field_descriptor_proto::Type::String),
        ];

        for item in data {
            let (rx, _tx) = async_std::channel::unbounded();

            let result = map_field(
                "name",
                &vec![TableFieldSchema {
                    name: "something".to_string(),
                    r#type: item.0.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                }],
                &SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("flow", "connector"),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
            );

            assert_eq!(result.1.len(), 1);
            assert_eq!(result.1["something"].table_type, item.0);
            assert_eq!(result.0.field[0].r#type, Some(item.1.into()))
        }
    }

    #[test]
    fn can_map_a_struct() {
        let (rx, _tx) = async_std::channel::unbounded();

        let result = map_field(
            "name",
            &vec![TableFieldSchema {
                name: "something".to_string(),
                r#type: TableType::Struct.into(),
                mode: Mode::Required.into(),
                fields: vec![TableFieldSchema {
                    name: "subfield_a".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                }],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &SinkContext {
                uid: Default::default(),
                alias: Alias::new("flow", "connector"),
                connector_type: Default::default(),
                quiescence_beacon: Default::default(),
                notifier: ConnectionLostNotifier::new(rx),
            },
        );

        assert_eq!(result.1.len(), 1);
        assert_eq!(result.1["something"].table_type, TableType::Struct);
        assert_eq!(
            result.0.field[0].r#type,
            Some(field_descriptor_proto::Type::Message.into())
        );
        assert_eq!(result.1["something"].subfields.len(), 1);
        assert_eq!(
            result.1["something"].subfields["subfield_a"].table_type,
            TableType::Int64
        )
    }

    #[test]
    fn encode_fails_on_type_mismatch() {
        let data = [
            (
                Value::String("asdf".into()),
                Field {
                    table_type: TableType::Int64,
                    tag: 1,
                    subfields: Default::default(),
                },
            ),
            (
                Value::Static(StaticNode::F64(1.243)),
                Field {
                    table_type: TableType::String,
                    tag: 2,
                    subfields: Default::default(),
                },
            ),
        ];

        for (value, field) in data {
            let mut result_data = vec![];

            let result = encode_field(&value, &field, &mut result_data);

            assert!(result.is_err());
        }
    }

    #[test]
    pub fn test_can_encode_stringy_types() {
        // NOTE: This test always passes the string "I" as the value to encode, this is not correct for some of the types (e.g. datetime),
        // but we still allow it, leaving the validation to BigQuery
        let data = [
            TableType::String,
            TableType::Date,
            TableType::Time,
            TableType::Datetime,
            TableType::Geography,
            TableType::Numeric,
            TableType::Bignumeric,
            TableType::Timestamp,
        ];

        for item in data {
            let mut result = vec![];
            assert!(
                encode_field(
                    &Value::String("I".into()),
                    &Field {
                        table_type: item,
                        tag: 123,
                        subfields: Default::default()
                    },
                    &mut result
                )
                .is_ok(),
                "TableType: {:?} did not encode correctly",
                item
            );

            assert_eq!([218u8, 7u8, 1u8, 73u8], result[..]);
        }
    }

    #[test]
    pub fn test_can_encode_a_struct() {
        let mut values = halfbrown::HashMap::new();
        values.insert("a".into(), Value::Static(StaticNode::I64(1)));
        values.insert("b".into(), Value::Static(StaticNode::I64(1024)));
        let input = Value::Object(Box::new(values));

        let mut subfields = HashMap::new();
        subfields.insert(
            "a".into(),
            Field {
                table_type: TableType::Int64,
                tag: 1,
                subfields: Default::default(),
            },
        );
        subfields.insert(
            "b".into(),
            Field {
                table_type: TableType::Int64,
                tag: 2,
                subfields: Default::default(),
            },
        );

        let field = Field {
            table_type: TableType::Struct,
            tag: 1024,
            subfields,
        };

        let mut result = Vec::new();
        assert!(encode_field(&input, &field, &mut result).is_ok());

        assert_eq!([130u8, 64u8, 5u8, 8u8, 1u8, 16u8, 128u8, 8u8], result[..])
    }

    #[test]
    pub fn can_encode_a_double() {
        let value = Value::Static(StaticNode::F64(1.2345));
        let field = Field {
            table_type: TableType::Double,
            tag: 2,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        assert_eq!(
            [17u8, 141u8, 151u8, 110u8, 18u8, 131u8, 192u8, 243u8, 63u8],
            result[..]
        );
    }

    #[test]
    pub fn can_encode_boolean() {
        let value = Value::Static(StaticNode::Bool(false));
        let field = Field {
            table_type: TableType::Bool,
            tag: 43,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        assert_eq!([216u8, 2u8, 0u8], result[..]);
    }

    #[test]
    pub fn can_encode_bytes() {
        let value = Value::Bytes(vec![0x1u8, 0x2u8, 0x3u8].into());
        let field = Field {
            table_type: TableType::Bytes,
            tag: 1,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        assert_eq!([10u8, 3u8, 1u8, 2u8, 3u8], result[..]);
    }

    #[test]
    pub fn can_encode_json() {
        let value = Value::Object(Box::new(halfbrown::HashMap::new()));
        let field = Field {
            table_type: TableType::Json,
            tag: 1,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        // json is currently not supported, so we expect the field to be skipped
        assert_eq!([] as [u8; 0], result[..]);
    }

    #[test]
    pub fn can_encode_interval() {
        let value = Value::String("".into());
        let field = Field {
            table_type: TableType::Interval,
            tag: 1,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        // interval is currently not supported, so we expect the field to be skipped
        assert_eq!([] as [u8; 0], result[..]);
    }

    #[test]
    pub fn can_skips_unspecified() {
        let value = Value::String("".into());
        let field = Field {
            table_type: TableType::Unspecified,
            tag: 1,
            subfields: Default::default(),
        };

        let mut result = Vec::new();
        assert!(encode_field(&value, &field, &mut result).is_ok());

        // Fields should never have the "Unspecified" type, if that happens best we can do is to log a warning and ignore them
        assert_eq!([] as [u8; 0], result[..]);
    }

    #[test]
    pub fn mapping_generates_a_correct_descriptor() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![
                TableFieldSchema {
                    name: "a".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
                TableFieldSchema {
                    name: "b".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
            ],
            &sink_context,
        );

        let descriptor = mapping.descriptor();
        assert_eq!(2, descriptor.field.len());
        assert_eq!(
            field_descriptor_proto::Type::Int64 as i32,
            descriptor.field[0].r#type.unwrap()
        );
        assert_eq!(
            field_descriptor_proto::Type::Int64 as i32,
            descriptor.field[1].r#type.unwrap()
        );
    }

    #[test]
    pub fn can_map_json_to_protobuf() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![
                TableFieldSchema {
                    name: "a".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
                TableFieldSchema {
                    name: "b".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
            ],
            &sink_context,
        );
        let mut fields = halfbrown::HashMap::new();
        fields.insert("a".into(), Value::Static(StaticNode::I64(12)));
        fields.insert("b".into(), Value::Static(StaticNode::I64(21)));
        let result = mapping.map(&Value::Object(Box::new(fields))).unwrap();

        assert_eq!([8u8, 12u8, 16u8, 21u8], result[..]);
    }

    #[test]
    fn map_field_ignores_fields_that_are_not_in_definition() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![
                TableFieldSchema {
                    name: "a".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
                TableFieldSchema {
                    name: "b".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                },
            ],
            &sink_context,
        );
        let mut fields = halfbrown::HashMap::new();
        fields.insert("a".into(), Value::Static(StaticNode::I64(12)));
        fields.insert("b".into(), Value::Static(StaticNode::I64(21)));
        fields.insert("c".into(), Value::Static(StaticNode::I64(33)));
        let result = mapping.map(&Value::Object(Box::new(fields))).unwrap();

        assert_eq!([8u8, 12u8, 16u8, 21u8], result[..]);
    }

    #[test]
    fn map_field_ignores_struct_fields_that_are_not_in_definition() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Struct.into(),
                mode: Mode::Required.into(),
                fields: vec![TableFieldSchema {
                    name: "x".to_string(),
                    r#type: TableType::Int64.into(),
                    mode: Mode::Required.into(),
                    fields: vec![],
                    description: "".to_string(),
                    max_length: 0,
                    precision: 0,
                    scale: 0,
                }],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &sink_context,
        );
        let mut inner_fields = halfbrown::HashMap::new();
        inner_fields.insert("x".into(), Value::Static(StaticNode::I64(10)));
        inner_fields.insert("y".into(), Value::Static(StaticNode::I64(10)));
        let mut fields = halfbrown::HashMap::new();
        fields.insert("a".into(), Value::Object(Box::new(inner_fields)));
        let result = mapping.map(&Value::Object(Box::new(fields))).unwrap();

        assert_eq!([10u8, 2u8, 8u8, 10u8], result[..]);
    }

    #[test]
    fn fails_on_bytes_type_mismatch() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Bytes.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &sink_context,
        );
        let mut fields = halfbrown::HashMap::new();
        fields.insert("a".into(), Value::Static(StaticNode::I64(12)));
        let result = mapping.map(&Value::Object(Box::new(fields)));

        if let Err(Error(ErrorKind::BigQueryTypeMismatch("bytes", x), _)) = result {
            assert_eq!(x, ValueType::I64);
        } else {
            assert!(false, "Bytes conversion did not fail on type mismatch");
        }
    }

    #[test]
    fn fails_if_the_event_is_not_an_object() {
        let (rx, _tx) = async_std::channel::unbounded();

        let sink_context = SinkContext {
            uid: Default::default(),
            alias: Alias::new("flow", "connector"),
            connector_type: Default::default(),
            quiescence_beacon: Default::default(),
            notifier: ConnectionLostNotifier::new(rx),
        };
        let mapping = JsonToProtobufMapping::new(
            &vec![TableFieldSchema {
                name: "a".to_string(),
                r#type: TableType::Bytes.into(),
                mode: Mode::Required.into(),
                fields: vec![],
                description: "".to_string(),
                max_length: 0,
                precision: 0,
                scale: 0,
            }],
            &sink_context,
        );
        let result = mapping.map(&Value::Static(StaticNode::I64(123)));

        if let Err(Error(ErrorKind::BigQueryTypeMismatch("object", x), _)) = result {
            assert_eq!(x, ValueType::I64);
        } else {
            assert!(false, "Mapping did not fail on non-object event");
        }
    }

    #[async_std::test]
    async fn sink_fails_if_config_is_missing() -> Result<()> {
        let config = literal!({
            "config": {}
        });

        let result =
            ConnectorHarness::new(function_name!(), &gbq::writer::Builder::default(), &config)
                .await;

        assert!(result.is_err());

        Ok(())
    }

    #[async_std::test]
    async fn on_event_fails_if_client_is_not_conected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "table_id": "doesnotmatter",
            "connect_timeout": 1000000,
            "request_timeout": 1000000
        }))
        .unwrap();

        let mut sink = GbqSink::new(config);

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("flow", "connector"),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &Alias::new("flow", "connector"),
                )
                .unwrap(),
                0,
            )
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[async_std::test]
    async fn on_event_fails_if_write_stream_is_not_conected() -> Result<()> {
        let (rx, _tx) = async_std::channel::unbounded();
        let config = Config::new(&literal!({
            "table_id": "doesnotmatter",
            "connect_timeout": 1000000,
            "request_timeout": 1000000
        }))
        .unwrap();

        let mut sink = GbqSink::new(config);
        sink.set_client(BigQueryWriteClient::with_interceptor(
            Channel::from_static("http://example.com").connect_lazy(),
            AuthInterceptor {
                token: Box::new(|| Ok(Arc::new(String::new()))),
            },
        ));

        let result = sink
            .on_event(
                "",
                Event::signal_tick(),
                &SinkContext {
                    uid: Default::default(),
                    alias: Alias::new("flow", "connector"),
                    connector_type: Default::default(),
                    quiescence_beacon: Default::default(),
                    notifier: ConnectionLostNotifier::new(rx),
                },
                &mut EventSerializer::new(
                    None,
                    CodecReq::Structured,
                    vec![],
                    &ConnectorType::from(""),
                    &Alias::new("flow", "connector"),
                )
                .unwrap(),
                0,
            )
            .await;

        assert!(result.is_err());
        Ok(())
    }
}
