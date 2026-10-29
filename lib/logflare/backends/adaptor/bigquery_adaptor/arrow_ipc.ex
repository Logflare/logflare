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

  Each field is `{name, type, required}`. Row keys are matched after replacing `.` with
  `_`. `:timestamp` values are Unix microseconds, `:string_map` values are JSON objects
  written as lists of `key`/`value` structs, and `:json_array` elements are written as
  JSON strings. Missing arrays and maps are written as empty lists.
  Returns `{:error, message}` when a row does not fit the schema.
  """
  @spec get_ipc_bytes_with_schema(binary(), [field_spec()], compression()) ::
          {:ok, binary(), [binary()]} | {:error, String.t()}
  def get_ipc_bytes_with_schema(_ndjson, _fields, _compression),
    do: :erlang.nif_error(:nif_not_loaded)
end
