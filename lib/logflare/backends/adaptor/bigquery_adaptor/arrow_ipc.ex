defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC do
  use Rustler, otp_app: :logflare, crate: "arrowipc_ex"

  def get_ipc_bytes(data, is_otel \\ false) do
    compression = Application.get_env(:logflare, :arrow_ipc_compression, :zstd)
    get_ipc_bytes(data, compression, is_otel)
  end

  # When your NIF is loaded, it will override this function.
  # compression must be one of: :zstd, :lz4, :none
  # is_otel indicates whether start_time/end_time (already unix microseconds)
  # should be encoded as Arrow Timestamp columns
  def get_ipc_bytes(_data_frame_json, _compression, _is_otel),
    do: :erlang.nif_error(:nif_not_loaded)
end
