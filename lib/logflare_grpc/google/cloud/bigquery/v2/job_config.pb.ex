defmodule Google.Cloud.Bigquery.V2.ScriptOptions.KeyResultStatementKind do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :KEY_RESULT_STATEMENT_KIND_UNSPECIFIED, 0
  field :LAST, 1
  field :FIRST_SELECT, 2
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationLoad.ColumnNameCharacterMap do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :COLUMN_NAME_CHARACTER_MAP_UNSPECIFIED, 0
  field :STRICT, 1
  field :V1, 2
  field :V2, 3
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationLoad.SourceColumnMatch do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :SOURCE_COLUMN_MATCH_UNSPECIFIED, 0
  field :POSITION, 1
  field :NAME, 2
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationTableCopy.OperationType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :OPERATION_TYPE_UNSPECIFIED, 0
  field :COPY, 1
  field :SNAPSHOT, 2
  field :RESTORE, 3
  field :CLONE, 4
end

defmodule Google.Cloud.Bigquery.V2.DestinationTableProperties.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.DestinationTableProperties do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :friendly_name, 1,
    type: Google.Protobuf.StringValue,
    json_name: "friendlyName",
    deprecated: false

  field :description, 2, type: Google.Protobuf.StringValue, deprecated: false

  field :labels, 3,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.DestinationTableProperties.LabelsEntry,
    map: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ConnectionProperty do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationQuery.ExternalTableDefinitionsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: Google.Cloud.Bigquery.V2.ExternalDataConfiguration
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationQuery do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :query, 1, type: :string

  field :destination_table, 2,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "destinationTable",
    deprecated: false

  field :external_table_definitions, 23,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.JobConfigurationQuery.ExternalTableDefinitionsEntry,
    json_name: "tableDefinitions",
    map: true,
    deprecated: false

  field :user_defined_function_resources, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.UserDefinedFunctionResource,
    json_name: "userDefinedFunctionResources"

  field :create_disposition, 5, type: :string, json_name: "createDisposition", deprecated: false
  field :write_disposition, 6, type: :string, json_name: "writeDisposition", deprecated: false

  field :default_dataset, 7,
    type: Google.Cloud.Bigquery.V2.DatasetReference,
    json_name: "defaultDataset",
    deprecated: false

  field :priority, 8, type: :string, deprecated: false

  field :allow_large_results, 10,
    type: Google.Protobuf.BoolValue,
    json_name: "allowLargeResults",
    deprecated: false

  field :use_query_cache, 11,
    type: Google.Protobuf.BoolValue,
    json_name: "useQueryCache",
    deprecated: false

  field :flatten_results, 12,
    type: Google.Protobuf.BoolValue,
    json_name: "flattenResults",
    deprecated: false

  field :maximum_bytes_billed, 14,
    type: Google.Protobuf.Int64Value,
    json_name: "maximumBytesBilled"

  field :use_legacy_sql, 15,
    type: Google.Protobuf.BoolValue,
    json_name: "useLegacySql",
    deprecated: false

  field :parameter_mode, 16, type: :string, json_name: "parameterMode"

  field :query_parameters, 17,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.QueryParameter,
    json_name: "queryParameters"

  field :system_variables, 35,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.SystemVariables,
    json_name: "systemVariables",
    deprecated: false

  field :schema_update_options, 18,
    repeated: true,
    type: :string,
    json_name: "schemaUpdateOptions"

  field :time_partitioning, 19,
    type: Google.Cloud.Bigquery.V2.TimePartitioning,
    json_name: "timePartitioning"

  field :range_partitioning, 22,
    type: Google.Cloud.Bigquery.V2.RangePartitioning,
    json_name: "rangePartitioning"

  field :clustering, 20, type: Google.Cloud.Bigquery.V2.Clustering

  field :destination_encryption_configuration, 21,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "destinationEncryptionConfiguration"

  field :script_options, 24,
    type: Google.Cloud.Bigquery.V2.ScriptOptions,
    json_name: "scriptOptions"

  field :connection_properties, 33,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ConnectionProperty,
    json_name: "connectionProperties"

  field :create_session, 34, type: Google.Protobuf.BoolValue, json_name: "createSession"
  field :continuous, 36, type: Google.Protobuf.BoolValue, deprecated: false

  field :write_incremental_results, 37,
    type: :bool,
    json_name: "writeIncrementalResults",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ScriptOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :statement_timeout_ms, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "statementTimeoutMs"

  field :statement_byte_budget, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "statementByteBudget"

  field :key_result_statement, 4,
    type: Google.Cloud.Bigquery.V2.ScriptOptions.KeyResultStatementKind,
    json_name: "keyResultStatement",
    enum: true
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationLoad do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_uris, 1, repeated: true, type: :string, json_name: "sourceUris"

  field :file_set_spec_type, 49,
    type: Google.Cloud.Bigquery.V2.FileSetSpecType,
    json_name: "fileSetSpecType",
    enum: true,
    deprecated: false

  field :schema, 2, type: Google.Cloud.Bigquery.V2.TableSchema, deprecated: false

  field :destination_table, 3,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "destinationTable"

  field :destination_table_properties, 4,
    type: Google.Cloud.Bigquery.V2.DestinationTableProperties,
    json_name: "destinationTableProperties",
    deprecated: false

  field :create_disposition, 5, type: :string, json_name: "createDisposition", deprecated: false
  field :write_disposition, 6, type: :string, json_name: "writeDisposition", deprecated: false

  field :null_marker, 7,
    type: Google.Protobuf.StringValue,
    json_name: "nullMarker",
    deprecated: false

  field :field_delimiter, 8, type: :string, json_name: "fieldDelimiter", deprecated: false

  field :skip_leading_rows, 9,
    type: Google.Protobuf.Int32Value,
    json_name: "skipLeadingRows",
    deprecated: false

  field :encoding, 10, type: :string, deprecated: false
  field :quote, 11, type: Google.Protobuf.StringValue, deprecated: false

  field :max_bad_records, 12,
    type: Google.Protobuf.Int32Value,
    json_name: "maxBadRecords",
    deprecated: false

  field :allow_quoted_newlines, 15,
    type: Google.Protobuf.BoolValue,
    json_name: "allowQuotedNewlines"

  field :source_format, 16, type: :string, json_name: "sourceFormat", deprecated: false

  field :allow_jagged_rows, 17,
    type: Google.Protobuf.BoolValue,
    json_name: "allowJaggedRows",
    deprecated: false

  field :ignore_unknown_values, 18,
    type: Google.Protobuf.BoolValue,
    json_name: "ignoreUnknownValues",
    deprecated: false

  field :projection_fields, 19, repeated: true, type: :string, json_name: "projectionFields"
  field :autodetect, 20, type: Google.Protobuf.BoolValue, deprecated: false

  field :schema_update_options, 21,
    repeated: true,
    type: :string,
    json_name: "schemaUpdateOptions"

  field :time_partitioning, 22,
    type: Google.Cloud.Bigquery.V2.TimePartitioning,
    json_name: "timePartitioning"

  field :range_partitioning, 26,
    type: Google.Cloud.Bigquery.V2.RangePartitioning,
    json_name: "rangePartitioning"

  field :clustering, 23, type: Google.Cloud.Bigquery.V2.Clustering

  field :destination_encryption_configuration, 24,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "destinationEncryptionConfiguration"

  field :use_avro_logical_types, 25,
    type: Google.Protobuf.BoolValue,
    json_name: "useAvroLogicalTypes",
    deprecated: false

  field :reference_file_schema_uri, 45,
    type: Google.Protobuf.StringValue,
    json_name: "referenceFileSchemaUri",
    deprecated: false

  field :hive_partitioning_options, 37,
    type: Google.Cloud.Bigquery.V2.HivePartitioningOptions,
    json_name: "hivePartitioningOptions",
    deprecated: false

  field :decimal_target_types, 39,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.DecimalTargetType,
    json_name: "decimalTargetTypes",
    enum: true

  field :json_extension, 41,
    type: Google.Cloud.Bigquery.V2.JsonExtension,
    json_name: "jsonExtension",
    enum: true,
    deprecated: false

  field :parquet_options, 42,
    type: Google.Cloud.Bigquery.V2.ParquetOptions,
    json_name: "parquetOptions",
    deprecated: false

  field :preserve_ascii_control_characters, 44,
    type: Google.Protobuf.BoolValue,
    json_name: "preserveAsciiControlCharacters",
    deprecated: false

  field :connection_properties, 46,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.ConnectionProperty,
    json_name: "connectionProperties",
    deprecated: false

  field :create_session, 47,
    type: Google.Protobuf.BoolValue,
    json_name: "createSession",
    deprecated: false

  field :column_name_character_map, 50,
    type: Google.Cloud.Bigquery.V2.JobConfigurationLoad.ColumnNameCharacterMap,
    json_name: "columnNameCharacterMap",
    enum: true,
    deprecated: false

  field :copy_files_only, 51,
    type: Google.Protobuf.BoolValue,
    json_name: "copyFilesOnly",
    deprecated: false

  field :time_zone, 52,
    type: Google.Protobuf.StringValue,
    json_name: "timeZone",
    deprecated: false

  field :null_markers, 53,
    repeated: true,
    type: :string,
    json_name: "nullMarkers",
    deprecated: false

  field :date_format, 54,
    proto3_optional: true,
    type: :string,
    json_name: "dateFormat",
    deprecated: false

  field :datetime_format, 55,
    proto3_optional: true,
    type: :string,
    json_name: "datetimeFormat",
    deprecated: false

  field :time_format, 56,
    proto3_optional: true,
    type: :string,
    json_name: "timeFormat",
    deprecated: false

  field :timestamp_format, 57,
    proto3_optional: true,
    type: :string,
    json_name: "timestampFormat",
    deprecated: false

  field :source_column_match, 58,
    type: Google.Cloud.Bigquery.V2.JobConfigurationLoad.SourceColumnMatch,
    json_name: "sourceColumnMatch",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationTableCopy do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_table, 1, type: Google.Cloud.Bigquery.V2.TableReference, json_name: "sourceTable"

  field :source_tables, 2,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "sourceTables"

  field :destination_table, 3,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "destinationTable"

  field :create_disposition, 4, type: :string, json_name: "createDisposition", deprecated: false
  field :write_disposition, 5, type: :string, json_name: "writeDisposition", deprecated: false

  field :destination_encryption_configuration, 6,
    type: Google.Cloud.Bigquery.V2.EncryptionConfiguration,
    json_name: "destinationEncryptionConfiguration"

  field :operation_type, 8,
    type: Google.Cloud.Bigquery.V2.JobConfigurationTableCopy.OperationType,
    json_name: "operationType",
    enum: true,
    deprecated: false

  field :destination_expiration_time, 9,
    type: Google.Protobuf.Timestamp,
    json_name: "destinationExpirationTime",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationExtract.ModelExtractOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :trial_id, 1, type: Google.Protobuf.Int64Value, json_name: "trialId"
end

defmodule Google.Cloud.Bigquery.V2.JobConfigurationExtract do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  oneof(:source, 0)

  field :source_table, 1,
    type: Google.Cloud.Bigquery.V2.TableReference,
    json_name: "sourceTable",
    oneof: 0

  field :source_model, 9,
    type: Google.Cloud.Bigquery.V2.ModelReference,
    json_name: "sourceModel",
    oneof: 0

  field :destination_uris, 3, repeated: true, type: :string, json_name: "destinationUris"

  field :print_header, 4,
    type: Google.Protobuf.BoolValue,
    json_name: "printHeader",
    deprecated: false

  field :field_delimiter, 5, type: :string, json_name: "fieldDelimiter", deprecated: false
  field :destination_format, 6, type: :string, json_name: "destinationFormat", deprecated: false
  field :compression, 7, type: :string, deprecated: false

  field :use_avro_logical_types, 13,
    type: Google.Protobuf.BoolValue,
    json_name: "useAvroLogicalTypes"

  field :model_extract_options, 14,
    type: Google.Cloud.Bigquery.V2.JobConfigurationExtract.ModelExtractOptions,
    json_name: "modelExtractOptions",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JobConfiguration.LabelsEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :key, 1, type: :string
  field :value, 2, type: :string
end

defmodule Google.Cloud.Bigquery.V2.JobConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :job_type, 8, type: :string, json_name: "jobType"
  field :query, 1, type: Google.Cloud.Bigquery.V2.JobConfigurationQuery
  field :load, 2, type: Google.Cloud.Bigquery.V2.JobConfigurationLoad
  field :copy, 3, type: Google.Cloud.Bigquery.V2.JobConfigurationTableCopy
  field :extract, 4, type: Google.Cloud.Bigquery.V2.JobConfigurationExtract
  field :dry_run, 5, type: Google.Protobuf.BoolValue, json_name: "dryRun", deprecated: false

  field :job_timeout_ms, 6,
    type: Google.Protobuf.Int64Value,
    json_name: "jobTimeoutMs",
    deprecated: false

  field :labels, 7,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.JobConfiguration.LabelsEntry,
    map: true

  field :reservation, 11, proto3_optional: true, type: :string, deprecated: false
end
