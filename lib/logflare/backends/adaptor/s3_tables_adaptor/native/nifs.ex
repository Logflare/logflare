defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Native.Nifs do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "s3_tables_ex"

  def init_catalog(_ref, _config), do: :erlang.nif_error(:nif_not_loaded)

  def ensure_table(_ref, _catalog, _table_name, _fields, _properties),
    do: :erlang.nif_error(:nif_not_loaded)

  def table_columns(_ref, _catalog, _table_name), do: :erlang.nif_error(:nif_not_loaded)

  def append_batch(_ref, _catalog, _table_name, _ndjson),
    do: :erlang.nif_error(:nif_not_loaded)

  def snapshot_info(_ref, _catalog, _table_name), do: :erlang.nif_error(:nif_not_loaded)
end
