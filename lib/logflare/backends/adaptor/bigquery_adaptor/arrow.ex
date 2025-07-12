defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.Arrow.Native do
  use Rustler, otp_app: :logflare, crate: "arrow_ipc_ex"

  def serialize_schema(_schema), do: :erlang.nif_error(:nif_not_loaded)
end
