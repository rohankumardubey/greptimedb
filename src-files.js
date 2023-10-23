var srcIndex = JSON.parse('{\
"api":["",[["v1",[],["column_def.rs"]]],["error.rs","helper.rs","lib.rs","v1.rs"]],\
"auth":["",[["user_provider",[],["static_user_provider.rs"]]],["common.rs","error.rs","lib.rs","permission.rs","user_info.rs","user_provider.rs"]],\
"catalog":["",[["information_schema",[],["columns.rs","tables.rs"]],["kvbackend",[],["client.rs","manager.rs"]],["memory",[],["manager.rs"]]],["error.rs","information_schema.rs","kvbackend.rs","lib.rs","memory.rs","metrics.rs","table_source.rs"]],\
"client":["",[],["client.rs","client_manager.rs","database.rs","error.rs","lib.rs","load_balance.rs","metrics.rs","region.rs","stream_insert.rs"]],\
"cmd":["",[["cli",[["bench",[],["metadata.rs"]]],["bench.rs","cmd.rs","export.rs","helper.rs","repl.rs","upgrade.rs"]]],["cli.rs","datanode.rs","error.rs","frontend.rs","lib.rs","metasrv.rs","options.rs","standalone.rs"]],\
"common_base":["",[],["bit_vec.rs","buffer.rs","bytes.rs","lib.rs","readable_size.rs"]],\
"common_catalog":["",[],["consts.rs","error.rs","lib.rs"]],\
"common_config":["",[],["lib.rs"]],\
"common_datasource":["",[["file_format",[],["csv.rs","json.rs","orc.rs","parquet.rs"]],["object_store",[],["fs.rs","s3.rs"]]],["buffered_writer.rs","compression.rs","error.rs","file_format.rs","lib.rs","lister.rs","object_store.rs","share_buffer.rs","util.rs"]],\
"common_error":["",[],["ext.rs","format.rs","lib.rs","mock.rs","status_code.rs"]],\
"common_function":["",[["scalars",[["aggregate",[],["argmax.rs","argmin.rs","diff.rs","mean.rs","percentile.rs","polyval.rs","scipy_stats_norm_cdf.rs","scipy_stats_norm_pdf.rs"]],["expression",[],["binary.rs","ctx.rs","unary.rs"]],["math",[],["pow.rs","rate.rs"]],["numpy",[],["clip.rs","interp.rs"]],["timestamp",[],["greatest.rs","to_unixtime.rs"]]],["aggregate.rs","expression.rs","function.rs","function_registry.rs","math.rs","numpy.rs","timestamp.rs","udf.rs"]]],["lib.rs","scalars.rs"]],\
"common_greptimedb_telemetry":["",[],["lib.rs"]],\
"common_grpc":["",[],["channel_manager.rs","error.rs","flight.rs","lib.rs","select.rs","writer.rs"]],\
"common_grpc_expr":["",[],["alter.rs","delete.rs","error.rs","insert.rs","lib.rs","util.rs"]],\
"common_macro":["",[],["aggr_func.rs","lib.rs","print_caller.rs","range_fn.rs","stack_trace_debug.rs"]],\
"common_mem_prof":["",[["jemalloc",[],["error.rs"]]],["error.rs","jemalloc.rs","lib.rs"]],\
"common_meta":["",[["ddl",[],["alter_table.rs","create_table.rs","drop_table.rs","truncate_table.rs","utils.rs"]],["heartbeat",[["handler",[],["parse_mailbox_message.rs"]]],["handler.rs","mailbox.rs","utils.rs"]],["key",[],["catalog_name.rs","datanode_table.rs","schema_name.rs","table_info.rs","table_name.rs","table_region.rs","table_route.rs"]],["kv_backend",[["txn",[],["etcd.rs"]]],["memory.rs","test.rs","txn.rs"]],["rpc",[],["ddl.rs","lock.rs","router.rs","store.rs","util.rs"]]],["cache_invalidator.rs","datanode_manager.rs","ddl.rs","ddl_manager.rs","distributed_time_constants.rs","error.rs","heartbeat.rs","instruction.rs","key.rs","kv_backend.rs","lib.rs","metrics.rs","peer.rs","range_stream.rs","rpc.rs","sequence.rs","state_store.rs","table_name.rs","util.rs"]],\
"common_procedure":["",[["local",[],["lock.rs","runner.rs"]],["store",[],["state_store.rs"]]],["error.rs","lib.rs","local.rs","options.rs","procedure.rs","store.rs","watcher.rs"]],\
"common_procedure_test":["",[],["lib.rs"]],\
"common_query":["",[["logical_plan",[],["accumulator.rs","expr.rs","udaf.rs","udf.rs"]]],["columnar_value.rs","error.rs","function.rs","lib.rs","logical_plan.rs","physical_plan.rs","prelude.rs","signature.rs"]],\
"common_recordbatch":["",[],["adapter.rs","error.rs","lib.rs","recordbatch.rs","util.rs"]],\
"common_runtime":["",[],["error.rs","global.rs","lib.rs","metrics.rs","repeated_task.rs","runtime.rs"]],\
"common_telemetry":["",[],["lib.rs","logging.rs","macros.rs","metric.rs","panic_hook.rs"]],\
"common_test_util":["",[],["lib.rs","ports.rs","temp_dir.rs"]],\
"common_time":["",[],["date.rs","datetime.rs","duration.rs","error.rs","interval.rs","lib.rs","range.rs","time.rs","timestamp.rs","timestamp_millis.rs","timezone.rs","util.rs"]],\
"common_version":["",[],["lib.rs"]],\
"datanode":["",[["heartbeat",[],["handler.rs"]],["store",[],["azblob.rs","fs.rs","gcs.rs","oss.rs","s3.rs"]]],["alive_keeper.rs","config.rs","datanode.rs","error.rs","event_listener.rs","greptimedb_telemetry.rs","heartbeat.rs","lib.rs","metrics.rs","region_server.rs","server.rs","store.rs"]],\
"datatypes":["",[["schema",[],["column_schema.rs","constraint.rs","raw.rs"]],["types",[],["binary_type.rs","boolean_type.rs","cast.rs","date_type.rs","datetime_type.rs","dictionary_type.rs","duration_type.rs","interval_type.rs","list_type.rs","null_type.rs","primitive_type.rs","string_type.rs","time_type.rs","timestamp_type.rs"]],["vectors",[["operations",[],["cast.rs","filter.rs","find_unique.rs","replicate.rs","take.rs"]]],["binary.rs","boolean.rs","constant.rs","date.rs","datetime.rs","duration.rs","eq.rs","helper.rs","interval.rs","list.rs","null.rs","operations.rs","primitive.rs","string.rs","time.rs","timestamp.rs","validity.rs"]]],["arrow_array.rs","data_type.rs","duration.rs","error.rs","interval.rs","lib.rs","macros.rs","prelude.rs","scalars.rs","schema.rs","serialize.rs","time.rs","timestamp.rs","type_id.rs","types.rs","value.rs","vectors.rs"]],\
"file_engine":["",[["query",[],["file_stream.rs"]]],["config.rs","engine.rs","error.rs","lib.rs","manifest.rs","query.rs","region.rs"]],\
"frontend":["",[["heartbeat",[["handler",[],["invalidate_table_cache.rs"]]],["handler.rs"]],["instance",[],["grpc.rs","influxdb.rs","opentsdb.rs","otlp.rs","prom_store.rs","region_query.rs","script.rs","standalone.rs"]],["service_config",[],["datanode.rs","grpc.rs","influxdb.rs","mysql.rs","opentsdb.rs","otlp.rs","postgres.rs","prom_store.rs"]]],["error.rs","frontend.rs","heartbeat.rs","instance.rs","lib.rs","metrics.rs","script.rs","server.rs","service_config.rs"]],\
"greptime":["",[],["greptime.rs"]],\
"log_store":["",[["raft_engine",[],["backend.rs","log_store.rs"]],["test_util",[],["log_store_util.rs"]]],["error.rs","lib.rs","noop.rs","raft_engine.rs","test_util.rs"]],\
"meta_client":["",[["client",[],["ask_leader.rs","ddl.rs","heartbeat.rs","load_balance.rs","lock.rs","router.rs","store.rs"]]],["client.rs","error.rs","lib.rs"]],\
"meta_srv":["",[["election",[],["etcd.rs"]],["handler",[["failure_handler",[],["runner.rs"]]],["check_leader_handler.rs","collect_stats_handler.rs","failure_handler.rs","filter_inactive_region_stats.rs","keep_lease_handler.rs","mailbox_handler.rs","node_stat.rs","on_leader_start_handler.rs","persist_stats_handler.rs","publish_heartbeat_handler.rs","region_lease_handler.rs","response_header_handler.rs"]],["lock",[],["etcd.rs","keys.rs","memory.rs"]],["metasrv",[],["builder.rs"]],["procedure",[["region_failover",[],["activate_region.rs","deactivate_region.rs","failover_end.rs","failover_start.rs","invalidate_cache.rs","update_metadata.rs"]]],["region_failover.rs","utils.rs"]],["pubsub",[],["publish.rs","subscribe_manager.rs","subscriber.rs"]],["selector",[],["lease_based.rs","load_based.rs"]],["service",[["admin",[],["health.rs","heartbeat.rs","inactive_regions.rs","leader.rs","meta.rs","node_lease.rs","route.rs","util.rs"]],["store",[],["cached_kv.rs","etcd.rs","etcd_util.rs","kv.rs","memory.rs"]]],["admin.rs","cluster.rs","ddl.rs","heartbeat.rs","lock.rs","mailbox.rs","router.rs","store.rs"]]],["bootstrap.rs","cache_invalidator.rs","cluster.rs","election.rs","error.rs","failure_detector.rs","greptimedb_telemetry.rs","handler.rs","inactive_region_manager.rs","keys.rs","lease.rs","lib.rs","lock.rs","metasrv.rs","metrics.rs","mocks.rs","procedure.rs","pubsub.rs","selector.rs","service.rs","table_meta_alloc.rs","table_routes.rs"]],\
"mito2":["",[["cache",[],["cache_size.rs"]],["compaction",[],["output.rs","picker.rs","twcs.rs"]],["manifest",[],["action.rs","manager.rs","storage.rs"]],["memtable",[],["key_values.rs","time_series.rs","version.rs"]],["read",[],["compat.rs","merge.rs","projection.rs","scan_region.rs","seq_scan.rs"]],["region",[],["opener.rs","options.rs","version.rs"]],["schedule",[],["scheduler.rs"]],["sst",[["parquet",[],["format.rs","reader.rs","row_group.rs","stats.rs","writer.rs"]]],["file.rs","file_purger.rs","parquet.rs","stream_writer.rs","version.rs"]],["worker",[],["handle_alter.rs","handle_close.rs","handle_compaction.rs","handle_create.rs","handle_drop.rs","handle_flush.rs","handle_open.rs","handle_truncate.rs","handle_write.rs"]]],["access_layer.rs","cache.rs","compaction.rs","config.rs","engine.rs","error.rs","flush.rs","lib.rs","manifest.rs","memtable.rs","metrics.rs","read.rs","region.rs","region_write_ctx.rs","request.rs","row_converter.rs","schedule.rs","sst.rs","wal.rs","worker.rs"]],\
"nyc_taxi":["",[],["nyc-taxi.rs"]],\
"object_store":["",[["layers",[["lru_cache",[],["read_cache.rs"]]],["lru_cache.rs"]]],["layers.rs","lib.rs","metrics.rs","test_util.rs","util.rs"]],\
"operator":["",[["req_convert",[["common",[],["partitioner.rs"]],["delete",[],["column_to_row.rs","row_to_region.rs","table_to_region.rs"]],["insert",[],["column_to_row.rs","row_to_region.rs","stmt_to_region.rs","table_to_region.rs"]]],["common.rs","delete.rs","insert.rs"]],["statement",[],["backup.rs","copy_table_from.rs","copy_table_to.rs","ddl.rs","describe.rs","dml.rs","show.rs","tql.rs"]]],["delete.rs","error.rs","expr_factory.rs","insert.rs","lib.rs","metrics.rs","region_req_factory.rs","req_convert.rs","statement.rs","table.rs"]],\
"partition":["",[],["columns.rs","error.rs","lib.rs","manager.rs","metrics.rs","partition.rs","range.rs","route.rs","splitter.rs"]],\
"plugins":["",[],["datanode.rs","frontend.rs","lib.rs","meta_srv.rs"]],\
"promql":["",[["extension_plan",[],["empty_metric.rs","histogram_fold.rs","instant_manipulate.rs","normalize.rs","planner.rs","range_manipulate.rs","series_divide.rs"]],["functions",[],["aggr_over_time.rs","changes.rs","deriv.rs","extrapolate_rate.rs","holt_winters.rs","idelta.rs","predict_linear.rs","quantile.rs","resets.rs"]]],["error.rs","extension_plan.rs","functions.rs","lib.rs","metrics.rs","planner.rs","range_array.rs"]],\
"query":["",[["datafusion",[],["error.rs","planner.rs"]],["dist_plan",[],["analyzer.rs","commutativity.rs","merge_scan.rs","planner.rs"]],["optimizer",[],["order_hint.rs","string_normalization.rs","type_conversion.rs"]],["query_engine",[],["context.rs","options.rs","state.rs"]],["range_select",[],["plan.rs","plan_rewrite.rs","planner.rs"]],["sql",[],["show_create_table.rs"]]],["dataframe.rs","datafusion.rs","dist_plan.rs","error.rs","executor.rs","lib.rs","logical_optimizer.rs","metrics.rs","optimizer.rs","parser.rs","physical_optimizer.rs","physical_planner.rs","physical_wrapper.rs","plan.rs","planner.rs","query_engine.rs","range_select.rs","region_query.rs","sql.rs","table_mutation.rs"]],\
"script":["",[["python",[["ffi_types",[["copr",[],["compile.rs","parse.rs"]]],["copr.rs","py_recordbatch.rs","utils.rs","vector.rs"]],["rspython",[],["builtins.rs","copr_impl.rs","dataframe_impl.rs","utils.rs","vector_impl.rs"]]],["engine.rs","error.rs","ffi_types.rs","metric.rs","rspython.rs","utils.rs"]]],["engine.rs","error.rs","lib.rs","manager.rs","python.rs","table.rs"]],\
"servers":["",[["grpc",[["flight",[],["stream.rs"]]],["database.rs","flight.rs","greptime_handler.rs","prom_query_gateway.rs","region_server.rs"]],["http",[],["authorize.rs","handler.rs","header.rs","influxdb.rs","mem_prof.rs","opentsdb.rs","otlp.rs","pprof.rs","prom_store.rs","prometheus.rs","script.rs"]],["metrics",[["jemalloc",[],["error.rs"]]],["jemalloc.rs"]],["mysql",[],["federated.rs","handler.rs","helper.rs","server.rs","writer.rs"]],["opentsdb",[],["codec.rs","connection.rs","handler.rs"]],["postgres",[["types",[],["interval.rs"]]],["auth_handler.rs","handler.rs","server.rs","types.rs"]],["query_handler",[],["grpc.rs","sql.rs"]]],["configurator.rs","error.rs","grpc.rs","heartbeat_options.rs","http.rs","influxdb.rs","interceptor.rs","lib.rs","line_writer.rs","metrics.rs","metrics_handler.rs","mysql.rs","opentsdb.rs","otlp.rs","postgres.rs","prom_store.rs","prometheus_handler.rs","query_handler.rs","row_writer.rs","server.rs","shutdown.rs","tls.rs"]],\
"session":["",[],["context.rs","lib.rs"]],\
"sql":["",[["parsers",[],["alter_parser.rs","copy_parser.rs","create_parser.rs","delete_parser.rs","describe_parser.rs","drop_parser.rs","explain_parser.rs","insert_parser.rs","query_parser.rs","show_parser.rs","tql_parser.rs","truncate_parser.rs"]],["statements",[["option_map",[],["visit.rs","visit_mut.rs"]],["transform",[],["type_alias.rs"]]],["alter.rs","copy.rs","create.rs","delete.rs","describe.rs","drop.rs","explain.rs","insert.rs","option_map.rs","query.rs","show.rs","statement.rs","tql.rs","transform.rs","truncate.rs"]]],["ast.rs","dialect.rs","error.rs","lib.rs","parser.rs","parsers.rs","statements.rs","util.rs"]],\
"sqlness_runner":["",[],["env.rs","main.rs","util.rs"]],\
"storage":["",[["compaction",[],["noop.rs","picker.rs","scheduler.rs","task.rs","twcs.rs","writer.rs"]],["flush",[],["picker.rs","scheduler.rs"]],["manifest",[],["action.rs","checkpoint.rs","helper.rs","impl_.rs","region.rs","storage.rs"]],["memtable",[],["btree.rs","inserter.rs","version.rs"]],["proto",[],["wal.rs"]],["read",[],["chain.rs","dedup.rs","merge.rs","windowed.rs"]],["region",[],["writer.rs"]],["scheduler",[],["dedup_deque.rs","rate_limit.rs"]],["schema",[],["compat.rs","projected.rs","region.rs","store.rs"]],["sst",[],["parquet.rs","pruning.rs","stream_writer.rs"]],["write_batch",[],["codec.rs","compat.rs"]]],["chunk.rs","codec.rs","compaction.rs","config.rs","engine.rs","error.rs","file_purger.rs","flush.rs","lib.rs","manifest.rs","memtable.rs","metadata.rs","metrics.rs","proto.rs","read.rs","region.rs","scheduler.rs","schema.rs","snapshot.rs","sst.rs","sync.rs","version.rs","wal.rs","window_infer.rs","write_batch.rs"]],\
"store_api":["",[["logstore",[],["entry.rs","entry_stream.rs","namespace.rs"]],["manifest",[],["action.rs","storage.rs"]],["storage",[],["chunk.rs","consts.rs","descriptors.rs","engine.rs","metadata.rs","region.rs","requests.rs","responses.rs","snapshot.rs","types.rs"]]],["data_source.rs","error.rs","lib.rs","logstore.rs","manifest.rs","metadata.rs","path_utils.rs","region_engine.rs","region_request.rs","storage.rs"]],\
"substrait":["",[],["df_substrait.rs","error.rs","extension_serializer.rs","lib.rs"]],\
"table":["",[["predicate",[],["stats.rs"]],["table",[],["adapter.rs","metrics.rs","numbers.rs","scan.rs"]],["test_util",[],["empty_table.rs","memtable.rs","mock_engine.rs","table_info.rs"]]],["dist_table.rs","engine.rs","error.rs","lib.rs","metadata.rs","predicate.rs","requests.rs","stats.rs","table.rs","test_util.rs","thin_table.rs"]],\
"tests_integration":["",[],["cluster.rs","grpc.rs","influxdb.rs","instance.rs","lib.rs","opentsdb.rs","otlp.rs","prom_store.rs","standalone.rs","test_util.rs"]]\
}');
createSrcSidebar();
