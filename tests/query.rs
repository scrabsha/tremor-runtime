// Copyright 2020-2021, The Tremor Team
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
use pretty_assertions::assert_eq;
use std::io::prelude::*;
use tremor_common::{file, ids::OperatorIdGen};
use tremor_pipeline::query::Query;
use tremor_pipeline::ExecutableGraph;
use tremor_pipeline::{Event, EventId};
use tremor_script::FN_REGISTRY;

use serial_test::serial;
use tremor_runtime::errors::*;
use tremor_script::module::Manager;
use tremor_script::utils::*;

fn to_pipe(query: String) -> Result<ExecutableGraph> {
    let aggr_reg = tremor_script::aggr_registry();
    let mut idgen = OperatorIdGen::new();
    let q = Query::parse(&query, &*FN_REGISTRY.read()?, &aggr_reg)?;
    Ok(q.to_pipe(&mut idgen)?)
}

macro_rules! test_cases {

    ($($file:ident),* ,) => {
        $(
            #[async_std::test]
            #[serial(query)]
            async fn $file() -> Result<()> {
                serial_test::set_max_wait(std::time::Duration::from_secs(120));

                tremor_runtime::functions::load()?;
                let query_dir = concat!("tests/queries/", stringify!($file), "/").to_string();
                let query_file = concat!("tests/queries/", stringify!($file), "/query.trickle");
                let in_file = concat!("tests/queries/", stringify!($file), "/in");
                let out_file = concat!("tests/queries/", stringify!($file), "/out");
                Manager::clear_path()?;
                Manager::add_path(&"tremor-script/lib")?;
                Manager::add_path(&query_dir)?;
                println!("Loading query: {}", query_file);
                let mut file = file::open(query_file)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                let mut pipeline = to_pipe(contents)?;

                println!("Loading input: {}", in_file);
                let in_json = load_event_file(in_file)?;
                println!("Loading expected: {}", out_file);
                let mut out_json = load_event_file(out_file)?;

                out_json.reverse();

                let mut results = Vec::new();
                for (id, json) in in_json.into_iter().enumerate() {
                    let event = Event {
                        id: EventId::new(0, 0, (id as u64), (id as u64)),
                        data: json.clone_static().into(),
                        ingest_ns: id as u64,
                        ..Event::default()
                    };
                    let mut r = vec![];
                    pipeline.enqueue("in", event, &mut r).await?;
                    results.append(&mut r);
                }
                assert_eq!(results.len(), out_json.len(), "Number of events differ error");
                for (_, result) in results {
                    for value in result.value_iter() {
                        let serialized = sorted_serialize(value)?;
                        if let Some(expected) = out_json.pop() {
                            assert_eq!(serialized, sorted_serialize(&expected)?);
                        }
                    }
                }
                Ok(())
            }
        )*
    };
}

test_cases!(
    binary_op_short_circuit,
    default_rule,
    dimensions,
    example_rule,
    group_by_size,
    group_by_time,
    group_country_region_az,
    group_each,
    group_set,
    having_filter,
    layered_limiting,
    lru,
    merge,
    multi_dimensions,
    mutate,
    simple_passthrough,
    patch,
    rewrite_root,
    script_params_overwrite,
    script_params,
    state,
    state_counter_operator,
    streams,
    tremor_map,
    where_filter,
    window_by_two_scripted,
    window_by_two,
    window_size_tilted,
    pp_win,
    pp_script,
    pp_operator,
    pp_alias_win,
    pp_alias_script,
    pp_alias_operator,
    pp_config_directive,
    // INSERT
    route_emit,
    drop_event,
    pipeline_group_by_size,
    pipeline_complex_args,
    pipeline_nested_script,
    pipeline_nested_operator,
    args_nesting_no_leakage,
    args_nesting_redefine,
    pipeline_nested_pipeline,
    pipeline_passthrough,
    alias_script_params_overwrite,
    cardinality,
    window_mixed_2,
    window_mixed_1,
    pp_const,
    pp_fn,
    script_error,
    guard_where,
    guard_having,
    history,
    roundrobin,
);
