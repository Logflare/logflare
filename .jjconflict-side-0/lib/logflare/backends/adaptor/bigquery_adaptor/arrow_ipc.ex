defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC do
  use Rustler, otp_app: :logflare, crate: "arrowipc_ex"

  def get_ipc_bytes(data) do
    compression = Application.get_env(:logflare, :arrow_ipc_compression, :zstd)
    get_ipc_bytes(data, compression)
  end

  # When your NIF is loaded, it will override this function.
  # compression must be one of: :zstd, :lz4, :none
  def get_ipc_bytes(_data_frame_json, _compression), do: :erlang.nif_error(:nif_not_loaded)
end
