defmodule Google.Cloud.Bigquery.V2.ExternalDataConfiguration.ObjectMetadata do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :OBJECT_METADATA_UNSPECIFIED, 0
  field :DIRECTORY, 1
  field :SIMPLE, 2
end

defmodule Google.Cloud.Bigquery.V2.ExternalDataConfiguration.MetadataCacheMode do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :METADATA_CACHE_MODE_UNSPECIFIED, 0
  field :AUTOMATIC, 1
  field :MANUAL, 2
end

defmodule Google.Cloud.Bigquery.V2.AvroOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :use_avro_logical_types, 1,
    type: Google.Protobuf.BoolValue,
    json_name: "useAvroLogicalTypes",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ParquetOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :enum_as_string, 1,
    type: Google.Protobuf.BoolValue,
    json_name: "enumAsString",
    deprecated: false

  field :enable_list_inference, 2,
    type: Google.Protobuf.BoolValue,
    json_name: "enableListInference",
    deprecated: false

  field :map_target_type, 3,
    type: Google.Cloud.Bigquery.V2.MapTargetType,
    json_name: "mapTargetType",
    enum: true,
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.CsvOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :field_delimiter, 1, type: :string, json_name: "fieldDelimiter", deprecated: false

  field :skip_leading_rows, 2,
    type: Google.Protobuf.Int64Value,
    json_name: "skipLeadingRows",
    deprecated: false

  field :quote, 3, type: Google.Protobuf.StringValue, deprecated: false

  field :allow_quoted_newlines, 4,
    type: Google.Protobuf.BoolValue,
    json_name: "allowQuotedNewlines",
    deprecated: false

  field :allow_jagged_rows, 5,
    type: Google.Protobuf.BoolValue,
    json_name: "allowJaggedRows",
    deprecated: false

  field :encoding, 6, type: :string, deprecated: false

  field :preserve_ascii_control_characters, 7,
    type: Google.Protobuf.BoolValue,
    json_name: "preserveAsciiControlCharacters",
    deprecated: false

  field :null_marker, 8,
    type: Google.Protobuf.StringValue,
    json_name: "nullMarker",
    deprecated: false

  field :null_markers, 9,
    repeated: true,
    type: :string,
    json_name: "nullMarkers",
    deprecated: false

  field :source_column_match, 10, type: :string, json_name: "sourceColumnMatch", deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.JsonOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :encoding, 1, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.BigtableColumn do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :qualifier_encoded, 1, type: Google.Protobuf.BytesValue, json_name: "qualifierEncoded"
  field :qualifier_string, 2, type: Google.Protobuf.StringValue, json_name: "qualifierString"
  field :field_name, 3, type: :string, json_name: "fieldName", deprecated: false
  field :type, 4, type: :string, deprecated: false
  field :encoding, 5, type: :string, deprecated: false

  field :only_read_latest, 6,
    type: Google.Protobuf.BoolValue,
    json_name: "onlyReadLatest",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.BigtableColumnFamily do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :family_id, 1, type: :string, json_name: "familyId"
  field :type, 2, type: :string, deprecated: false
  field :encoding, 3, type: :string, deprecated: false

  field :columns, 4,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.BigtableColumn,
    deprecated: false

  field :only_read_latest, 5,
    type: Google.Protobuf.BoolValue,
    json_name: "onlyReadLatest",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.BigtableOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :column_families, 1,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.BigtableColumnFamily,
    json_name: "columnFamilies",
    deprecated: false

  field :ignore_unspecified_column_families, 2,
    type: Google.Protobuf.BoolValue,
    json_name: "ignoreUnspecifiedColumnFamilies",
    deprecated: false

  field :read_rowkey_as_string, 3,
    type: Google.Protobuf.BoolValue,
    json_name: "readRowkeyAsString",
    deprecated: false

  field :output_column_families_as_json, 4,
    type: Google.Protobuf.BoolValue,
    json_name: "outputColumnFamiliesAsJson",
    deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.GoogleSheetsOptions do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :skip_leading_rows, 1,
    type: Google.Protobuf.Int64Value,
    json_name: "skipLeadingRows",
    deprecated: false

  field :range, 2, type: :string, deprecated: false
end

defmodule Google.Cloud.Bigquery.V2.ExternalDataConfiguration do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :source_uris, 1, repeated: true, type: :string, json_name: "sourceUris"

  field :file_set_spec_type, 25,
    type: Google.Cloud.Bigquery.V2.FileSetSpecType,
    json_name: "fileSetSpecType",
    enum: true,
    deprecated: false

  field :schema, 2, type: Google.Cloud.Bigquery.V2.TableSchema, deprecated: false
  field :source_format, 3, type: :string, json_name: "sourceFormat"

  field :max_bad_records, 4,
    type: Google.Protobuf.Int32Value,
    json_name: "maxBadRecords",
    deprecated: false

  field :autodetect, 5, type: Google.Protobuf.BoolValue

  field :ignore_unknown_values, 6,
    type: Google.Protobuf.BoolValue,
    json_name: "ignoreUnknownValues",
    deprecated: false

  field :compression, 7, type: :string, deprecated: false

  field :csv_options, 8,
    type: Google.Cloud.Bigquery.V2.CsvOptions,
    json_name: "csvOptions",
    deprecated: false

  field :json_options, 26,
    type: Google.Cloud.Bigquery.V2.JsonOptions,
    json_name: "jsonOptions",
    deprecated: false

  field :bigtable_options, 9,
    type: Google.Cloud.Bigquery.V2.BigtableOptions,
    json_name: "bigtableOptions",
    deprecated: false

  field :google_sheets_options, 10,
    type: Google.Cloud.Bigquery.V2.GoogleSheetsOptions,
    json_name: "googleSheetsOptions",
    deprecated: false

  field :hive_partitioning_options, 13,
    type: Google.Cloud.Bigquery.V2.HivePartitioningOptions,
    json_name: "hivePartitioningOptions",
    deprecated: false

  field :connection_id, 14, type: :string, json_name: "connectionId", deprecated: false

  field :decimal_target_types, 16,
    repeated: true,
    type: Google.Cloud.Bigquery.V2.DecimalTargetType,
    json_name: "decimalTargetTypes",
    enum: true

  field :avro_options, 17,
    type: Google.Cloud.Bigquery.V2.AvroOptions,
    json_name: "avroOptions",
    deprecated: false

  field :json_extension, 18,
    type: Google.Cloud.Bigquery.V2.JsonExtension,
    json_name: "jsonExtension",
    enum: true,
    deprecated: false

  field :parquet_options, 19,
    type: Google.Cloud.Bigquery.V2.ParquetOptions,
    json_name: "parquetOptions",
    deprecated: false

  field :object_metadata, 22,
    proto3_optional: true,
    type: Google.Cloud.Bigquery.V2.ExternalDataConfiguration.ObjectMetadata,
    json_name: "objectMetadata",
    enum: true,
    deprecated: false

  field :reference_file_schema_uri, 23,
    type: Google.Protobuf.StringValue,
    json_name: "referenceFileSchemaUri",
    deprecated: false

  field :metadata_cache_mode, 24,
    type: Google.Cloud.Bigquery.V2.ExternalDataConfiguration.MetadataCacheMode,
    json_name: "metadataCacheMode",
    enum: true,
    deprecated: false

  field :time_zone, 27,
    proto3_optional: true,
    type: :string,
    json_name: "timeZone",
    deprecated: false

  field :date_format, 28,
    proto3_optional: true,
    type: :string,
    json_name: "dateFormat",
    deprecated: false

  field :datetime_format, 29,
    proto3_optional: true,
    type: :string,
    json_name: "datetimeFormat",
    deprecated: false

  field :time_format, 30,
    proto3_optional: true,
    type: :string,
    json_name: "timeFormat",
    deprecated: false

  field :timestamp_format, 31,
    proto3_optional: true,
    type: :string,
    json_name: "timestampFormat",
    deprecated: false
end
