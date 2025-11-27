defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC do
  use Rustler, otp_app: :logflare, crate: "arrowipc_ex"

  # When your NIF is loaded, it will override this function.
  def get_ipc_bytes(_data_frame_json), do: :erlang.nif_error(:nif_not_loaded)
end
