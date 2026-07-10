defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager do
  @moduledoc """
  Permanent process that provisions the S3 Tables catalog and Iceberg tables for a backend on
  boot, then caches the resulting catalog resource for lock-free hot-path reads.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source

  @doc false
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @doc false
  @spec start_link(S3TablesAdaptor.source_backend_tuple()) :: GenServer.on_start()
  def start_link({%Source{}, %Backend{}} = args) do
    GenServer.start_link(__MODULE__, args, name: via(args))
  end

  @doc """
  Returns the cached catalog resource for a provisioned backend.

  Reads `:persistent_term` directly, bypassing this GenServer.
  """
  @spec fetch_catalog(pos_integer()) :: {:ok, reference()} | {:error, :not_provisioned}
  def fetch_catalog(backend_id) when is_integer(backend_id) do
    case :persistent_term.get(pt_key(backend_id), nil) do
      nil -> {:error, :not_provisioned}
      catalog -> {:ok, catalog}
    end
  end

  @doc false
  @impl GenServer
  def init({%Source{}, %Backend{}} = args) do
    {:ok, args, {:continue, :provision}}
  end

  @doc false
  @impl GenServer
  def handle_continue(:provision, {source, backend} = state) do
    if :persistent_term.get(pt_key(backend.id), nil) do
      {:noreply, state}
    else
      case provision(backend) do
        :ok ->
          {:noreply, state}

        {:error, reason} = error ->
          Logger.warning("S3 Tables catalog provisioning failed",
            source_id: source.id,
            backend_id: backend.id,
            error_string: inspect(reason)
          )

          {:stop, {:shutdown, error}, state}
      end
    end
  end

  @spec provision(Backend.t()) :: :ok | {:error, term()}
  defp provision(%Backend{} = backend) do
    config = Adaptor.get_backend_config(backend)

    with {:ok, catalog} <- Native.init_catalog(config),
         :ok <- ensure_tables(catalog) do
      :persistent_term.put(pt_key(backend.id), catalog)
      :ok
    end
  end

  @spec ensure_tables(reference()) :: :ok | {:error, term()}
  defp ensure_tables(catalog) do
    Enum.reduce_while(IcebergSchema.event_types(), :ok, fn event_type, :ok ->
      table_name = IcebergSchema.table_name(event_type)
      fields = IcebergSchema.fields(event_type)

      case Native.ensure_table(catalog, table_name, fields) do
        {:ok, _} -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  # FIXME: This leaks a CatalogResource per backend_id
  @doc false
  @impl GenServer
  def terminate(_reason, _state), do: :ok

  @spec via(S3TablesAdaptor.source_backend_tuple()) :: S3TablesAdaptor.via_tuple()
  defp via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, __MODULE__, backend)
  end

  @spec pt_key(pos_integer()) :: {module(), pos_integer()}
  defp pt_key(backend_id), do: {__MODULE__, backend_id}
end
