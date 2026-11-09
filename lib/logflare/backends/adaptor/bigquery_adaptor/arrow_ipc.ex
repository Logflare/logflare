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
  using the given fields as the schema instead of inferring one from the rows.

  Each field is `{name, type, required}`. Row keys must already be the column names,
  and `:json_array` elements must already be JSON strings, as in the mapper's NDJSON
  output with `column_names: :underscore, nested_arrays: :json_strings`. `:timestamp`
  values are Unix microseconds, `:string_map` values are JSON objects written as lists
  of `key`/`value` structs, and missing arrays and maps are written as empty lists.

  Rows are split into one record batch per `max_batch_bytes` of input, so each batch
  fits an AppendRows request (10 MB). The default of 5 MB leaves headroom because
  Arrow can be larger than the input JSON, e.g. for many short attribute values. A
  single row larger than `max_batch_bytes` gets a batch of its own.

  Returns `{:error, message}` when a row does not fit the schema.
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
