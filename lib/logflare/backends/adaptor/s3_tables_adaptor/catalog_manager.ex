defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager do
  @moduledoc """
  Permanent per-backend process that provisions the S3 Tables catalog and Iceberg tables on
  boot, then caches the resulting catalog resource for lock-free hot-path reads.

  For existing tables, provisioning compares the live column names against `IcebergSchema` and
  logs a warning on drift.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry
  alias Logflare.LogEvent.TypeDetection

  @doc false
  def child_spec(backend) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc false
  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @doc """
  Returns the cached catalog resource for a provisioned backend.

  Reads the manager's `Registry` value directly, bypassing the GenServer.
  """
  @spec fetch_catalog(pos_integer()) :: {:ok, reference()} | {:error, :not_provisioned}
  def fetch_catalog(backend_id) when is_integer(backend_id) do
    case Registry.lookup(BackendRegistry, {__MODULE__, backend_id}) do
      [{_pid, catalog}] when is_reference(catalog) -> {:ok, catalog}
      _ -> {:error, :not_provisioned}
    end
  end

  @doc false
  @impl GenServer
  def init(%Backend{} = backend) do
    {:ok, %{backend: backend, catalog: nil}, {:continue, :provision}}
  end

  @doc false
  @impl GenServer
  def handle_continue(:provision, %{backend: backend} = state) do
    case provision(backend) do
      {:ok, catalog} ->
        Registry.update_value(BackendRegistry, {__MODULE__, backend.id}, fn _ -> catalog end)
        {:noreply, %{state | catalog: catalog}}

      {:error, reason} = error ->
        Logger.warning("S3 Tables catalog provisioning failed",
          backend_id: backend.id,
          error_string: inspect(reason)
        )

        {:stop, {:shutdown, error}, state}
    end
  end

  @spec provision(Backend.t()) :: {:ok, reference()} | {:error, term()}
  defp provision(%Backend{} = backend) do
    config = Adaptor.get_backend_config(backend)

    with {:ok, catalog} <- Native.init_catalog(config),
         :ok <- ensure_tables(catalog, backend) do
      {:ok, catalog}
    end
  end

  @spec ensure_tables(reference(), Backend.t()) :: :ok | {:error, term()}
  defp ensure_tables(catalog, backend) do
    Enum.reduce_while(IcebergSchema.event_types(), :ok, fn event_type, :ok ->
      table_name = IcebergSchema.table_name(event_type)
      fields = IcebergSchema.fields(event_type)
      properties = IcebergSchema.table_properties(event_type)

      case Native.ensure_table(catalog, table_name, fields, properties) do
        {:ok, :created} ->
          {:cont, :ok}

        {:ok, :already_exists} ->
          warn_on_schema_drift(catalog, event_type, table_name, backend)
          {:cont, :ok}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  @spec warn_on_schema_drift(
          reference(),
          TypeDetection.event_type(),
          String.t(),
          Backend.t()
        ) :: :ok
  defp warn_on_schema_drift(catalog, event_type, table_name, backend) do
    with {:ok, info} <- Native.table_info(catalog, table_name),
         :ok <- check_version(info, event_type) do
      :ok
    else
      {:error, :version_mismatch, error_meta} ->
        metadata = [backend_id: backend.id, table_name: table_name] ++ error_meta
        Logger.warning("S3 Tables Iceberg table schema mismatch", metadata)

      {:error, reason} ->
        Logger.warning("S3 Tables Iceberg table schema check failed",
          backend_id: backend.id,
          table_name: table_name,
          error_string: inspect(reason)
        )
    end

    :ok
  end

  defp check_version(info, event_type) do
    expected_version = IcebergSchema.schema_version(event_type)
    expected_columns = Enum.map(IcebergSchema.fields(event_type), & &1.name)
    stored_version = info.properties["logflare.schema-version"]

    if stored_version == expected_version do
      :ok
    else
      error_meta = [
        missing_columns: expected_columns -- info.columns,
        extra_columns: info.columns -- expected_columns
      ]

      {:error, :version_mismatch, error_meta}
    end
  end
end
