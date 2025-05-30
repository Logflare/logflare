defmodule Google.Cloud.Bigquery.V2.ReservationEdition do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :RESERVATION_EDITION_UNSPECIFIED, 0
  field :STANDARD, 1
  field :ENTERPRISE, 2
  field :ENTERPRISE_PLUS, 3
end

defmodule Google.Cloud.Bigquery.V2.ExplainQueryStage.ComputeMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :COMPUTE_MODE_UNSPECIFIED, 0
  field :BIGQUERY, 1
  field :BI_ENGINE, 2
end

defmodule Google.Cloud.Bigquery.V2.BiEngineReason.Code do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :CODE_UNSPECIFIED, 0
  field :NO_RESERVATION, 1
  field :INSUFFICIENT_RESERVATION, 2
  field :UNSUPPORTED_SQL_TEXT, 4
  field :INPUT_TOO_LARGE, 5
  field :OTHER_REASON, 6
  field :TABLE_EXCLUDED, 7
end

defmodule Google.Cloud.Bigquery.V2.BiEngineStatistics.BiEngineMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :ACCELERATION_MODE_UNSPECIFIED, 0
  field :DISABLED, 1
  field :PARTIAL, 2
  field :FULL, 3
end

defmodule Google.Cloud.Bigquery.V2.BiEngineStatistics.BiEngineAccelerationMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :BI_ENGINE_ACCELERATION_MODE_UNSPECIFIED, 0
  field :BI_ENGINE_DISABLED, 1
  field :PARTIAL_INPUT, 2
  field :FULL_INPUT, 3
  field :FULL_QUERY, 4
end

defmodule Google.Cloud.Bigquery.V2.IndexUnusedReason.Code do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :CODE_UNSPECIFIED, 0
  field :INDEX_CONFIG_NOT_AVAILABLE, 1
  field :PENDING_INDEX_CREATION, 2
  field :BASE_TABLE_TRUNCATED, 3
  field :INDEX_CONFIG_MODIFIED, 4
  field :TIME_TRAVEL_QUERY, 5
  field :NO_PRUNING_POWER, 6
  field :UNINDEXED_SEARCH_FIELDS, 7
  field :UNSUPPORTED_SEARCH_PATTERN, 8
  field :OPTIMIZED_WITH_MATERIALIZED_VIEW, 9
  field :SECURED_BY_DATA_MASKING, 11
  field :MISMATCHED_TEXT_ANALYZER, 12
  field :BASE_TABLE_TOO_SMALL, 13
  field :BASE_TABLE_TOO_LARGE, 14
  field :ESTIMATED_PERFORMANCE_GAIN_TOO_LOW, 15
  field :NOT_SUPPORTED_IN_STANDARD_EDITION, 17
  field :INDEX_SUPPRESSED_BY_FUNCTION_OPTION, 18
  field :QUERY_CACHE_HIT, 19
  field :STALE_INDEX, 20
  field :INTERNAL_ERROR, 10
  field :OTHER_REASON, 16
end

defmodule Google.Cloud.Bigquery.V2.StoredColumnsUsage.StoredColumnsUnusedReason.Code do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :CODE_UNSPECIFIED, 0
  field :STORED_COLUMNS_COVER_INSUFFICIENT, 1
  field :BASE_TABLE_HAS_RLS, 2
  field :BASE_TABLE_HAS_CLS, 3
  field :UNSUPPORTED_PREFILTER, 4
  field :INTERNAL_ERROR, 5
  field :OTHER_REASON, 6
end

defmodule Google.Cloud.Bigquery.V2.SearchStatistics.IndexUsageMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :INDEX_USAGE_MODE_UNSPECIFIED, 0
  field :UNUSED, 1
  field :PARTIALLY_USED, 2
  field :FULLY_USED, 4
end

defmodule Google.Cloud.Bigquery.V2.VectorSearchStatistics.IndexUsageMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :INDEX_USAGE_MODE_UNSPECIFIED, 0
  field :UNUSED, 1
  field :PARTIALLY_USED, 2
  field :FULLY_USED, 4
end

defmodule Google.Cloud.Bigquery.V2.MlStatistics.TrainingType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :TRAINING_TYPE_UNSPECIFIED, 0
  field :SINGLE_TRAINING, 1
  field :HPARAM_TUNING, 2
end

defmodule Google.Cloud.Bigquery.V2.ScriptStatistics.EvaluationKind do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :EVALUATION_KIND_UNSPECIFIED, 0
  field :STATEMENT, 1
  field :EXPRESSION, 2
end

defmodule Google.Cloud.Bigquery.V2.MaterializedView.RejectedReason do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :REJECTED_REASON_UNSPECIFIED, 0
  field :NO_DATA, 1
  field :COST, 2
  field :BASE_TABLE_TRUNCATED, 3
  field :BASE_TABLE_DATA_CHANGE, 4
  field :BASE_TABLE_PARTITION_EXPIRATION_CHANGE, 5
  field :BASE_TABLE_EXPIRED_PARTITION, 6
  field :BASE_TABLE_INCOMPATIBLE_METADATA_CHANGE, 7
  field :TIME_ZONE, 8
  field :OUT_OF_TIME_TRAVEL_WINDOW, 9
  field :BASE_TABLE_FINE_GRAINED_SECURITY_POLICY, 10
  field :BASE_TABLE_TOO_STALE, 11
end

defmodule Google.Cloud.Bigquery.V2.TableMetadataCacheUsage.UnusedReason do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :UNUSED_REASON_UNSPECIFIED, 0
  field :EXCEEDED_MAX_STALENESS, 1
  field :METADATA_CACHING_NOT_ENABLED, 3
  field :OTHER_REASON, 2
end

defmodule Google.Cloud.Bigquery.V2.ExplainQueryStep do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :kind, 1, type: :string
  field :substeps, 2, repeated: true, type: :string
end

defmodule Google.Cloud.Bigquery.V2.ExplainQueryStage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :name, 1, type: :string
  field :id, 2, type: Google.Protobuf.Int64Value
  field :start_ms, 3, type: :int64, json_name: "startMs"
  field :end_ms, 4, type: :int64, json_name: "endMs"
  field :input_stages, 5, repeated: true, type: :int64, json_name: "inputStages"
  field :wait_ratio_avg, 6, type: Google.Protobuf.DoubleValue, json_name: "waitRatioAvg"
  field :wait_ms_avg, 7, type: Google.Protobuf.Int64Value, json_name: "waitMsAvg"
  field :wait_ratio_max, 8, type: Google.Protobuf.DoubleValue, json_name: "waitRatioMax"
  field :wait_ms_max, 9, type: Google.Protobuf.Int64Value, json_name: "waitMsMax"
  field :read_ratio_avg, 10, type: Google.Protobuf.DoubleValue, json_name: "readRatioAvg"
  field :read_ms_avg, 11, type: Google.Protobuf.Int64Value, json_name: "readMsAvg"
  field :read_ratio_max, 12, type: Google.Protobuf.DoubleValue, json_name: "readRatioMax"
  field :read_ms_max, 13, type: Google.Protobuf.Int64Value, json_name: "readMsMax"
  field :compute_ratio_avg, 14, type: Google.Protobuf.DoubleValue, json_name: "computeRatioAvg"
  field :compute_ms_avg, 15, type: Google.Protobuf.Int64Value, json_name: "computeMsAvg"
  field :compute_ratio_max, 16, type: Google.Protobuf.DoubleValue, json_name: "computeRatioMax"
  field :compute_ms_max, 17, type: Google.Protobuf.Int64Value, json_name: "computeMsMax"
  field :write_ratio_avg, 18, type: Google.Protobuf.DoubleValue, json_name: "writeRatioAvg"
  field :write_ms_avg, 19, type: Google.Protobuf.Int64Value, json_name: "writeMsAvg"
  field :write_ratio_max, 20, type: Google.Protobuf.DoubleValue, json_name: "writeRatioMax"
  field :write_ms_max, 21, type: Google.Protobuf.Int64Value, json_name: "writeMsMax"

  field :shuffle_output_bytes, 22,
    type: Google.Protobuf.Int64Value,
    json_name: "shuffleOutputBytes"

  field :shuffle_output_bytes_spilled, 23,
    type: Google.Protobuf.Int64Value,
    json_name: "shuffleOutputBytesSpilled"

  field :records_read, 24, type: Google.Protobuf.Int64Value, json_name: "recordsRead"
  field :records_written, 25, type: Google.Protobuf.Int64Value, json_name: "recordsWritten"
  field :parallel_inputs, 26, type: Google.Protobuf.Int64Value, json_name: "parallelInputs"

  field :completed_parallel_inputs, 27,
    type: Google.Protobuf.Int64Value,
    json_name: "completedParallelInputs"

  field :status, 28, type: :string
  field :steps, 29, repeated: true, type: Google.Cloud.Bigquery.V2.ExplainQueryStep
  field :slot_ms, 30, type: Google.Protobuf.Int64Value, json_name: "slotMs"

  field :compute_mode, 31,
    type: Google.Cloud.Bigquery.V2.ExplainQueryStage.ComputeMode,
    json_name: "computeMode",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.QueryTimelineSample do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :elapsed_ms, 1, type: Google.Protobuf.Int64Value, json_name: "elapsedMs"
  field :total_slot_ms, 2, type: Google.Protobuf.Int64Value, json_name: "totalSlotMs"
  field :pending_units, 3, type: Google.Protobuf.Int64Value, json_name: "pendingUnits"
  field :completed_units, 4, type: Google.Protobuf.Int64Value, json_name: "completedUnits"
  field :active_units, 5, type: Google.Protobuf.Int64Value, json_name: "activeUnits"

  field :shuffle_ram_usage_ratio, 6,
    type: Google.Protobuf.DoubleValue,
    json_name: "shuffleRamUsageRatio"

  field :estimated_runnable_units, 7,
    type: Google.Protobuf.Int64Value,
    json_name: "estimatedRunnableUnits"
end

defmodule Google.Cloud.Bigquery.V2.ExternalServiceCost do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :external_service, 1, type: :string, json_name: "externalService"
  field :bytes_processed, 2, type: Google.Protobuf.Int64Value, json_name: "bytesProcessed"
  field :bytes_billed, 3, type: Google.Protobuf.Int64Value, json_name: "bytesBilled"
  field :slot_ms, 4, type: Google.Protobuf.Int64Value, json_name: "slotMs"
  field :reserved_slot_count, 5, type: :int64, json_name: "reservedSlotCount"
end

defmodule Google.Cloud.Bigquery.V2.ExportDataStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :file_count, 1, type: Google.Protobuf.Int64Value, json_name: "fileCount"
  field :row_count, 2, type: Google.Protobuf.Int64Value, json_name: "rowCount"
end

defmodule Google.Cloud.Bigquery.V2.BiEngineReason do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :code, 1,
    type: Google.Cloud.Bigquery.V2.BiEngineReason.Code,
    enum: true,
    deprecated: false

  field :message, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.BiEngineStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :bi_engine_mode, 1,
    type: Google.Cloud.Bigquery.V2.BiEngineStatistics.BiEngineMode,
    json_name: "biEngineMode",
    enum: true,
    deprecated: false

  field :acceleration_mode, 3,
    type: Google.Cloud.Bigquery.V2.BiEngineStatistics.BiEngineAccelerationMode,
    json_name: "accelerationMode",
    enum: true,
    deprecated: false

  field :bi_engine_reasons, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.BiEngineReason,
    json_name: "biEngineReasons"
end

defmodule Google.Cloud.Bigquery.V2.IndexUnusedReason do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :code, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.IndexUnusedReason.Code,
    enum: true

  field :message, 2, proto3_optional: true, type: :string

  field :base_table, 3,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "baseTable"

  field :index_name, 4, proto3_optional: true, type: :string, json_name: "indexName"
end

defmodule Google.Cloud.Bigquery.V2.StoredColumnsUsage.StoredColumnsUnusedReason do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :code, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.StoredColumnsUsage.StoredColumnsUnusedReason.Code,
    enum: true

  field :message, 2, proto3_optional: true, type: :string
  field :uncovered_columns, 3, repeated: true, type: :string, json_name: "uncoveredColumns"
end

defmodule Google.Cloud.Bigquery.V2.StoredColumnsUsage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :is_query_accelerated, 1,
    proto3_optional: true,
    type: :bool,
    json_name: "isQueryAccelerated"

  field :base_table, 2,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "baseTable"

  field :stored_columns_unused_reasons, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StoredColumnsUsage.StoredColumnsUnusedReason,
    json_name: "storedColumnsUnusedReasons"
end

defmodule Google.Cloud.Bigquery.V2.SearchStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :index_usage_mode, 1,
    type: Google.Cloud.Bigquery.V2.SearchStatistics.IndexUsageMode,
    json_name: "indexUsageMode",
    enum: true

  field :index_unused_reasons, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.IndexUnusedReason,
    json_name: "indexUnusedReasons"
end

defmodule Google.Cloud.Bigquery.V2.VectorSearchStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :index_usage_mode, 1,
    type: Google.Cloud.Bigquery.V2.VectorSearchStatistics.IndexUsageMode,
    json_name: "indexUsageMode",
    enum: true

  field :index_unused_reasons, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.IndexUnusedReason,
    json_name: "indexUnusedReasons"

  field :stored_columns_usages, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StoredColumnsUsage,
    json_name: "storedColumnsUsages"
end

defmodule Google.Cloud.Bigquery.V2.QueryInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :optimization_details, 2,
    type: Google.Protobuf.Struct,
    json_name: "optimizationDetails",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.LoadQueryStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :input_files, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "inputFiles",
    deprecated: false

  field :input_file_bytes, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "inputFileBytes",
    deprecated: false

  field :output_rows, 3,
    type: Google.Protobuf.Int64Value,
    json_name: "outputRows",
    deprecated: false

  field :output_bytes, 4,
    type: Google.Protobuf.Int64Value,
    json_name: "outputBytes",
    deprecated: false

  field :bad_records, 5,
    type: Google.Protobuf.Int64Value,
    json_name: "badRecords",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobStatistics2 do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query_plan, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ExplainQueryStage,
    json_name: "queryPlan",
    deprecated: false

  field :estimated_bytes_processed, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "estimatedBytesProcessed",
    deprecated: false

  field :timeline, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryTimelineSample,
    deprecated: false

  field :total_partitions_processed, 4,
    type: Google.Protobuf.Int64Value,
    json_name: "totalPartitionsProcessed",
    deprecated: false

  field :total_bytes_processed, 5,
    type: Google.Protobuf.Int64Value,
    json_name: "totalBytesProcessed",
    deprecated: false

  field :total_bytes_processed_accuracy, 21,
    type: :string,
    json_name: "totalBytesProcessedAccuracy",
    deprecated: false

  field :total_bytes_billed, 6,
    type: Google.Protobuf.Int64Value,
    json_name: "totalBytesBilled",
    deprecated: false

  field :billing_tier, 7,
    type: Google.Protobuf.Int32Value,
    json_name: "billingTier",
    deprecated: false

  field :total_slot_ms, 8,
    type: Google.Protobuf.Int64Value,
    json_name: "totalSlotMs",
    deprecated: false

  field :cache_hit, 9, type: Google.Protobuf.BoolValue, json_name: "cacheHit", deprecated: false

  field :referenced_tables, 10,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "referencedTables",
    deprecated: false

  field :referenced_routines, 24,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.RoutineReference,
    json_name: "referencedRoutines",
    deprecated: false

  field :schema, 11, type: Google.Cloud.Bigquery.V2.TableSchema, deprecated: false

  field :num_dml_affected_rows, 12,
    type: Google.Protobuf.Int64Value,
    json_name: "numDmlAffectedRows",
    deprecated: false

  field :dml_stats, 32,
    type: Google.Cloud.Bigquery.V2.DmlStats,
    json_name: "dmlStats",
    deprecated: false

  field :undeclared_query_parameters, 13,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryParameter,
    json_name: "undeclaredQueryParameters",
    deprecated: false

  field :statement_type, 14, type: :string, json_name: "statementType", deprecated: false

  field :ddl_operation_performed, 15,
    type: :string,
    json_name: "ddlOperationPerformed",
    deprecated: false

  field :ddl_target_table, 16,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "ddlTargetTable",
    deprecated: false

  field :ddl_destination_table, 31,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "ddlDestinationTable",
    deprecated: false

  field :ddl_target_row_access_policy, 26,
    type: Google.Cloud.Bigquery.V2.RowAccessPolicyReference,
    json_name: "ddlTargetRowAccessPolicy",
    deprecated: false

  field :ddl_affected_row_access_policy_count, 27,
    type: Google.Protobuf.Int64Value,
    json_name: "ddlAffectedRowAccessPolicyCount",
    deprecated: false

  field :ddl_target_routine, 22,
    type: Google.Cloud.Bigquery.V2.RoutineReference,
    json_name: "ddlTargetRoutine",
    deprecated: false

  field :ddl_target_dataset, 30,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "ddlTargetDataset",
    deprecated: false

  field :ml_statistics, 23,
    type: Google.Cloud.Bigquery.V2.MlStatistics,
    json_name: "mlStatistics",
    deprecated: false

  field :export_data_statistics, 25,
    type: Google.Cloud.Bigquery.V2.ExportDataStatistics,
    json_name: "exportDataStatistics",
    deprecated: false

  field :external_service_costs, 28,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ExternalServiceCost,
    json_name: "externalServiceCosts",
    deprecated: false

  field :bi_engine_statistics, 29,
    type: Google.Cloud.Bigquery.V2.BiEngineStatistics,
    json_name: "biEngineStatistics",
    deprecated: false

  field :load_query_statistics, 33,
    type: Google.Cloud.Bigquery.V2.LoadQueryStatistics,
    json_name: "loadQueryStatistics",
    deprecated: false

  field :dcl_target_table, 34,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "dclTargetTable",
    deprecated: false

  field :dcl_target_view, 35,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "dclTargetView",
    deprecated: false

  field :dcl_target_dataset, 36,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "dclTargetDataset",
    deprecated: false

  field :search_statistics, 37,
    type: Google.Cloud.Bigquery.V2.SearchStatistics,
    json_name: "searchStatistics",
    deprecated: false

  field :vector_search_statistics, 44,
    type: Google.Cloud.Bigquery.V2.VectorSearchStatistics,
    json_name: "vectorSearchStatistics",
    deprecated: false

  field :performance_insights, 38,
    type: Google.Cloud.Bigquery.V2.PerformanceInsights,
    json_name: "performanceInsights",
    deprecated: false

  field :query_info, 39,
    type: Google.Cloud.Bigquery.V2.QueryInfo,
    json_name: "queryInfo",
    deprecated: false

  field :spark_statistics, 40,
    type: Google.Cloud.Bigquery.V2.SparkStatistics,
    json_name: "sparkStatistics",
    deprecated: false

  field :transferred_bytes, 41,
    type: Google.Protobuf.Int64Value,
    json_name: "transferredBytes",
    deprecated: false

  field :materialized_view_statistics, 42,
    type: Google.Cloud.Bigquery.V2.MaterializedViewStatistics,
    json_name: "materializedViewStatistics",
    deprecated: false

  field :metadata_cache_statistics, 43,
    type: Google.Cloud.Bigquery.V2.MetadataCacheStatistics,
    json_name: "metadataCacheStatistics",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobStatistics3 do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :input_files, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "inputFiles",
    deprecated: false

  field :input_file_bytes, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "inputFileBytes",
    deprecated: false

  field :output_rows, 3,
    type: Google.Protobuf.Int64Value,
    json_name: "outputRows",
    deprecated: false

  field :output_bytes, 4,
    type: Google.Protobuf.Int64Value,
    json_name: "outputBytes",
    deprecated: false

  field :bad_records, 5,
    type: Google.Protobuf.Int64Value,
    json_name: "badRecords",
    deprecated: false

  field :timeline, 7,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryTimelineSample,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobStatistics4 do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :destination_uri_file_counts, 1,
    repeated: true,
    type: :int64,
    json_name: "destinationUriFileCounts",
    deprecated: false

  field :input_bytes, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "inputBytes",
    deprecated: false

  field :timeline, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryTimelineSample,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.CopyJobStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :copied_rows, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "copiedRows",
    deprecated: false

  field :copied_logical_bytes, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "copiedLogicalBytes",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.MlStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :max_iterations, 1, type: :int64, json_name: "maxIterations", deprecated: false

  field :iteration_results, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.TrainingRun.IterationResult,
    json_name: "iterationResults"

  field :model_type, 3,
    type: Google.Cloud.Bigquery.V2.Model.ModelType,
    json_name: "modelType",
    enum: true,
    deprecated: false

  field :training_type, 4,
    type: Google.Cloud.Bigquery.V2.MlStatistics.TrainingType,
    json_name: "trainingType",
    enum: true,
    deprecated: false

  field :hparam_trials, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.Model.HparamTuningTrial,
    json_name: "hparamTrials",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ScriptStatistics.ScriptStackFrame do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :start_line, 1, type: :int32, json_name: "startLine", deprecated: false
  field :start_column, 2, type: :int32, json_name: "startColumn", deprecated: false
  field :end_line, 3, type: :int32, json_name: "endLine", deprecated: false
  field :end_column, 4, type: :int32, json_name: "endColumn", deprecated: false
  field :procedure_id, 5, type: :string, json_name: "procedureId", deprecated: false
  field :text, 6, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ScriptStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :evaluation_kind, 1,
    type: Google.Cloud.Bigquery.V2.ScriptStatistics.EvaluationKind,
    json_name: "evaluationKind",
    enum: true

  field :stack_frames, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ScriptStatistics.ScriptStackFrame,
    json_name: "stackFrames"
end

defmodule Google.Cloud.Bigquery.V2.RowLevelSecurityStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :row_level_security_applied, 1, type: :bool, json_name: "rowLevelSecurityApplied"
end

defmodule Google.Cloud.Bigquery.V2.DataMaskingStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :data_masking_applied, 1, type: :bool, json_name: "dataMaskingApplied"
end

defmodule Google.Cloud.Bigquery.V2.JobStatistics.TransactionInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :transaction_id, 1, type: :string, json_name: "transactionId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :creation_time, 1, type: :int64, json_name: "creationTime", deprecated: false
  field :start_time, 2, type: :int64, json_name: "startTime", deprecated: false
  field :end_time, 3, type: :int64, json_name: "endTime", deprecated: false

  field :total_bytes_processed, 4,
    type: Google.Protobuf.Int64Value,
    json_name: "totalBytesProcessed",
    deprecated: false

  field :completion_ratio, 5,
    type: Google.Protobuf.DoubleValue,
    json_name: "completionRatio",
    deprecated: false

  field :quota_deferments, 9,
    repeated: true,
    type: :string,
    json_name: "quotaDeferments",
    deprecated: false

  field :query, 6, type: Google.Cloud.Bigquery.V2.JobStatistics2, deprecated: false
  field :load, 7, type: Google.Cloud.Bigquery.V2.JobStatistics3, deprecated: false
  field :extract, 8, type: Google.Cloud.Bigquery.V2.JobStatistics4, deprecated: false
  field :copy, 21, type: Google.Cloud.Bigquery.V2.CopyJobStatistics, deprecated: false

  field :total_slot_ms, 10,
    type: Google.Protobuf.Int64Value,
    json_name: "totalSlotMs",
    deprecated: false

  field :reservation_id, 15, type: :string, deprecated: false
  field :num_child_jobs, 12, type: :int64, json_name: "numChildJobs", deprecated: false
  field :parent_job_id, 13, type: :string, json_name: "parentJobId", deprecated: false

  field :script_statistics, 14,
    type: Google.Cloud.Bigquery.V2.ScriptStatistics,
    json_name: "scriptStatistics",
    deprecated: false

  field :row_level_security_statistics, 16,
    type: Google.Cloud.Bigquery.V2.RowLevelSecurityStatistics,
    json_name: "rowLevelSecurityStatistics",
    deprecated: false

  field :data_masking_statistics, 20,
    type: Google.Cloud.Bigquery.V2.DataMaskingStatistics,
    json_name: "dataMaskingStatistics",
    deprecated: false

  field :transaction_info, 17,
    type: Google.Cloud.Bigquery.V2.JobStatistics.TransactionInfo,
    json_name: "transactionInfo",
    deprecated: false

  field :session_info, 18,
    type: Google.Cloud.Bigquery.V2.SessionInfo,
    json_name: "sessionInfo",
    deprecated: false

  field :final_execution_duration_ms, 22,
    type: :int64,
    json_name: "finalExecutionDurationMs",
    deprecated: false

  field :edition, 24,
    type: Google.Cloud.Bigquery.V2.ReservationEdition,
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.DmlStats do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :inserted_row_count, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "insertedRowCount",
    deprecated: false

  field :deleted_row_count, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "deletedRowCount",
    deprecated: false

  field :updated_row_count, 3,
    type: Google.Protobuf.Int64Value,
    json_name: "updatedRowCount",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PerformanceInsights do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :avg_previous_execution_ms, 1,
    type: :int64,
    json_name: "avgPreviousExecutionMs",
    deprecated: false

  field :stage_performance_standalone_insights, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StagePerformanceStandaloneInsight,
    json_name: "stagePerformanceStandaloneInsights",
    deprecated: false

  field :stage_performance_change_insights, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.StagePerformanceChangeInsight,
    json_name: "stagePerformanceChangeInsights",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.StagePerformanceChangeInsight do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :stage_id, 1, type: :int64, json_name: "stageId", deprecated: false

  field :input_data_change, 2,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.InputDataChange,
    json_name: "inputDataChange",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.InputDataChange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :records_read_diff_percentage, 1,
    type: :float,
    json_name: "recordsReadDiffPercentage",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.StagePerformanceStandaloneInsight do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :stage_id, 1, type: :int64, json_name: "stageId", deprecated: false

  field :slot_contention, 2,
    proto3_optional: true,
    type: :bool,
    json_name: "slotContention",
    deprecated: false

  field :insufficient_shuffle_quota, 3,
    proto3_optional: true,
    type: :bool,
    json_name: "insufficientShuffleQuota",
    deprecated: false

  field :bi_engine_reasons, 5,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.BiEngineReason,
    json_name: "biEngineReasons",
    deprecated: false

  field :high_cardinality_joins, 6,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.HighCardinalityJoin,
    json_name: "highCardinalityJoins",
    deprecated: false

  field :partition_skew, 7,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.PartitionSkew,
    json_name: "partitionSkew",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.HighCardinalityJoin do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :left_rows, 1, type: :int64, json_name: "leftRows", deprecated: false
  field :right_rows, 2, type: :int64, json_name: "rightRows", deprecated: false
  field :output_rows, 3, type: :int64, json_name: "outputRows", deprecated: false
  field :step_index, 4, type: :int32, json_name: "stepIndex", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PartitionSkew.SkewSource do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :stage_id, 1, type: :int64, json_name: "stageId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.PartitionSkew do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :skew_sources, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.PartitionSkew.SkewSource,
    json_name: "skewSources",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.SparkStatistics.LoggingInfo do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource_type, 1, type: :string, json_name: "resourceType", deprecated: false
  field :project_id, 2, type: :string, json_name: "projectId", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.SparkStatistics.EndpointsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.SparkStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :spark_job_id, 1,
    proto3_optional: true,
    type: :string,
    json_name: "sparkJobId",
    deprecated: false

  field :spark_job_location, 2,
    proto3_optional: true,
    type: :string,
    json_name: "sparkJobLocation",
    deprecated: false

  field :endpoints, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.SparkStatistics.EndpointsEntry,
    map: true,
    deprecated: false

  field :logging_info, 4,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.SparkStatistics.LoggingInfo,
    json_name: "loggingInfo",
    deprecated: false

  field :kms_key_name, 5,
    proto3_optional: true,
    type: :string,
    json_name: "kmsKeyName",
    deprecated: false

  field :gcs_staging_bucket, 6,
    proto3_optional: true,
    type: :string,
    json_name: "gcsStagingBucket",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.MaterializedViewStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :materialized_view, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.MaterializedView,
    json_name: "materializedView"
end

defmodule Google.Cloud.Bigquery.V2.MaterializedView do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table_reference, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "tableReference"

  field :chosen, 2, proto3_optional: true, type: :bool

  field :estimated_bytes_saved, 3,
    proto3_optional: true,
    type: :int64,
    json_name: "estimatedBytesSaved"

  field :rejected_reason, 4,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.MaterializedView.RejectedReason,
    json_name: "rejectedReason",
    enum: true
end

defmodule Google.Cloud.Bigquery.V2.TableMetadataCacheUsage do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table_reference, 1,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "tableReference"

  field :unused_reason, 2,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.TableMetadataCacheUsage.UnusedReason,
    json_name: "unusedReason",
    enum: true

  field :explanation, 3, proto3_optional: true, type: :string
  field :staleness, 5, type: Google.Protobuf.Duration
  field :table_type, 6, type: :string, json_name: "tableType"
end

defmodule Google.Cloud.Bigquery.V2.MetadataCacheStatistics do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :table_metadata_cache_usage, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TableMetadataCacheUsage,
    json_name: "tableMetadataCacheUsage"
end
