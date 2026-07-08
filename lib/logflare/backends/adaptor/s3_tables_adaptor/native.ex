defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Native do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "s3_tables_ex"

  @spec init_catalog(map()) :: {:ok, reference()} | {:error, String.t()}
  def init_catalog(_config), do: :erlang.nif_error(:nif_not_loaded)

  @spec ensure_table(reference(), String.t(), String.t()) :: :ok | {:error, String.t()}
  def ensure_table(_catalog, _table_name, _schema_json), do: :erlang.nif_error(:nif_not_loaded)

  @spec append_batch(reference(), binary()) :: :ok | {:error, String.t()}
  def append_batch(_catalog, _arrow_ipc), do: :erlang.nif_error(:nif_not_loaded)
end
