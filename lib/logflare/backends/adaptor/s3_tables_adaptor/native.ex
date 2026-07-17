defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.Native do
  @moduledoc false
  # Public interface over the `s3_tables_ex` NIF.
  # The raw NIF stubs live in `Native.Nifs`; keeping them separate allows
  # mocking with Mimic

  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native.Nifs

  @default_timeout 5_000
  # must exceed the Rust-side APPEND_TIMEOUT (55s) so the NIF's own timeout
  # error arrives before the receive gives up
  @append_timeout 60_000

  @spec init_catalog(map()) :: {:ok, reference()} | {:error, String.t()}
  def init_catalog(config) do
    fn ref -> Nifs.init_catalog(ref, config) end
    |> wrap_sending_nif()
  end

  @spec ensure_table(reference(), String.t(), [map()], %{String.t() => String.t()}) ::
          {:ok, :created | :already_exists} | {:error, String.t()}
  def ensure_table(catalog, table_name, fields, properties) do
    fn ref -> Nifs.ensure_table(ref, catalog, table_name, fields, properties) end
    |> wrap_sending_nif()
  end

  @spec table_columns(reference(), String.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def table_columns(catalog, table_name) do
    fn ref -> Nifs.table_columns(ref, catalog, table_name) end
    |> wrap_sending_nif()
  end

  @spec append_batch(reference(), String.t(), binary()) ::
          {:ok, %{row_count: non_neg_integer(), data_files: non_neg_integer()}}
          | {:error, :commit_conflict | :timeout | String.t()}
  def append_batch(catalog, table_name, ndjson) do
    fn ref -> Nifs.append_batch(ref, catalog, table_name, ndjson) end
    |> wrap_sending_nif(@append_timeout)
  end

  @spec snapshot_info(reference(), String.t()) :: {:ok, map() | nil} | {:error, String.t()}
  def snapshot_info(catalog, table_name) do
    fn ref -> Nifs.snapshot_info(ref, catalog, table_name) end
    |> wrap_sending_nif()
  end

  defp wrap_sending_nif(nif_fun, timeout \\ @default_timeout) do
    ref = make_ref()
    :ok = nif_fun.(ref)

    receive do
      {^ref, result} -> result
    after
      # TODO: this may leak late messages to a caller, consider spawning a task
      timeout -> {:error, :timeout}
    end
  end
end
