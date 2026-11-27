defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC do
  use Rustler, otp_app: :logflare, crate: "arrowipc_ex"

  @type compression :: :zstd | :lz4 | :none
  @type field_type ::
          :string
          | :integer
          | :float
          | :boolean
          | :timestamp
          | :string_map
          | :integer_array
          | :float_array
          | :string_array
          | :json_array
  @type field_spec :: {String.t(), field_type(), boolean()}

  @default_max_batch_bytes 5 * 1024 * 1024

  def get_ipc_bytes(data) do
    compression = Application.get_env(:logflare, :arrow_ipc_compression, :zstd)
    get_ipc_bytes(data, compression)
  end

  # When your NIF is loaded, it will override this function.
  # compression must be one of: :zstd, :lz4, :none
  def get_ipc_bytes(_data_frame_json, _compression), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Encodes NDJSON rows into an Arrow IPC schema message and record batch messages,
  using `fields` (`{name, type, required}`) as the schema.

  Expects the mapper's NDJSON output with `column_names: :underscore` and
  `nested_arrays: :json_strings`; keys are not renamed here. `:timestamp` values are
  Unix microseconds, `:string_map` objects become `key`/`value` lists, and missing
  lists and maps become empty lists.

  Other input can fail the whole batch with `{:error, message}`: non-string values in
  string columns or maps, `null` list items, missing required fields, non-object rows.
  Floats in `:integer` and `:timestamp` columns are truncated, and numeric strings in
  `:integer` columns are parsed.

  Batches hold at most `max_batch_bytes` of input (default 5 MB, half the 10 MB
  AppendRows limit, since Arrow can be larger than its JSON input).
  """
  @spec get_ipc_bytes_with_schema(binary(), [field_spec()], compression(), pos_integer()) ::
          {:ok, binary(), [binary()]} | {:error, String.t()}
  def get_ipc_bytes_with_schema(
        ndjson,
        fields,
        compression,
        max_batch_bytes \\ @default_max_batch_bytes
      )

  def get_ipc_bytes_with_schema(_ndjson, _fields, _compression, _max_batch_bytes),
    do: :erlang.nif_error(:nif_not_loaded)
end
