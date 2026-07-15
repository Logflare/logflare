defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Native do
  @moduledoc false

  use Rustler, otp_app: :logflare, crate: "s3_tables_ex"

  @spec init_catalog(map()) :: {:ok, reference()} | {:error, String.t()}
  def init_catalog(config) do
    fn ref -> init_catalog_nif(ref, config) end
    |> wrap_sending_nif()
  end

  def init_catalog_nif(_ref, _config), do: :erlang.nif_error(:nif_not_loaded)

  @spec ensure_table(reference(), String.t(), [map()], %{String.t() => String.t()}) ::
          {:ok, :created | :already_exists} | {:error, String.t()}
  def ensure_table(catalog, table_name, fields, properties) do
    fn ref -> ensure_table_nif(ref, catalog, table_name, fields, properties) end
    |> wrap_sending_nif()
  end

  def ensure_table_nif(_ref, _catalog, _table_name, _fields, _properties),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec table_columns(reference(), String.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def table_columns(catalog, table_name) do
    fn ref -> table_columns_nif(ref, catalog, table_name) end
    |> wrap_sending_nif()
  end

  def table_columns_nif(_ref, _catalog, _table_name), do: :erlang.nif_error(:nif_not_loaded)

  @spec append_batch(reference(), binary()) :: :ok | {:error, String.t()}
  def append_batch(catalog, arrow_ipc) do
    fn ref -> append_batch_nif(ref, catalog, arrow_ipc) end
    |> wrap_sending_nif()
  end

  def append_batch_nif(_ref, _catalog, _arrow_ipc), do: :erlang.nif_error(:nif_not_loaded)

  defp wrap_sending_nif(nif_fun) do
    ref = make_ref()
    :ok = nif_fun.(ref)

    receive do
      {^ref, result} -> result
    after
      # TODO: this may leak late messages to a caller
      5000 -> {:error, :timeout}
    end
  end
end
