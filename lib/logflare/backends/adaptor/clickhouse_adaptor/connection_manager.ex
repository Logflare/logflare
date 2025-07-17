defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager do
  @moduledoc """
  Manages ClickHouse connection lifecycle for both ingest and query connections.

  Connection types:
  - `:ingest` - for write operations (log ingestion)
  - `:query` - for read operations (endpoint queries)
  """

  use GenServer
  use TypedStruct
  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Source

  @type source_backend_tuple :: {Source.t(), Backend.t()}
  @type connection_type :: :ingest | :query

  # Activity thresholds (following BigQuery patterns)
  @ingest_inactivity_timeout :timer.minutes(5)
  @query_inactivity_timeout :timer.minutes(5)
  @resolve_interval :timer.seconds(30)

  typedstruct do
    field :source, Source.t(), enforce: true
    field :backend, Backend.t(), enforce: true
    field :ingest_ch_opts, Keyword.t(), enforce: true
    field :query_ch_opts, Keyword.t(), enforce: true
    field :ingest_connection_pid, pid() | nil, default: nil
    field :query_connection_pid, pid() | nil, default: nil
    field :ingest_last_activity, integer() | nil, default: nil
    field :query_last_activity, integer() | nil, default: nil
    field :resolve_timer, reference() | nil, default: nil
  end

  defguardp is_connection_type(type) when type in ~w(ingest query)a

  @doc false
  @spec start_link(
          {Source.t(), Backend.t(), ingest_ch_opts :: Keyword.t(), query_ch_opts :: Keyword.t()}
        ) :: GenServer.on_start()
  def start_link({%Source{} = source, %Backend{} = backend, ingest_ch_opts, query_ch_opts})
      when is_list(ingest_ch_opts) and is_list(query_ch_opts) do
    GenServer.start_link(__MODULE__, {source, backend, ingest_ch_opts, query_ch_opts},
      name: connection_manager_via({source, backend})
    )
  end

  @doc false
  @spec child_spec(
          {Source.t(), Backend.t(), ingest_ch_opts :: Keyword.t(), query_ch_opts :: Keyword.t()}
        ) :: Supervisor.child_spec()
  def child_spec({%Source{} = source, %Backend{} = backend, ingest_ch_opts, query_ch_opts}) do
    %{
      id: {__MODULE__, {source.id, backend.id}},
      start: {__MODULE__, :start_link, [{source, backend, ingest_ch_opts, query_ch_opts}]}
    }
  end

  @doc """
  Notifies the connection manager of ingest activity.

  This resets the inactivity timer for ingest connections.
  """
  @spec notify_ingest_activity(Source.t(), Backend.t()) :: :ok
  def notify_ingest_activity(%Source{} = source, %Backend{} = backend) do
    manager = connection_manager_via({source, backend})
    GenServer.cast(manager, {:activity, :ingest})
  end

  @doc """
  Notifies the connection manager of query activity.

  This resets the inactivity timer for query connections.
  """
  @spec notify_query_activity(Source.t(), Backend.t()) :: :ok
  def notify_query_activity(%Source{} = source, %Backend{} = backend) do
    manager = connection_manager_via({source, backend})
    GenServer.cast(manager, {:activity, :query})
  end

  @doc """
  Ensures a connection of the specified type is started.

  This will start the connection if it's not already running and notify the manager of activity.
  """
  @spec ensure_connection_started(Source.t(), Backend.t(), connection_type()) ::
          :ok | {:error, term()}
  def ensure_connection_started(%Source{} = source, %Backend{} = backend, connection_type)
      when is_connection_type(connection_type) do
    manager = connection_manager_via({source, backend})
    GenServer.call(manager, {:ensure_connection, connection_type})
  end

  @doc """
  Checks if a connection of the specified type is currently active.
  """
  @spec connection_active?(Source.t(), Backend.t(), connection_type()) :: boolean()
  def connection_active?(%Source{} = source, %Backend{} = backend, connection_type)
      when is_connection_type(connection_type) do
    manager = connection_manager_via({source, backend})

    try do
      GenServer.call(manager, {:connection_active?, connection_type})
    catch
      :exit, _ -> false
    end
  end

  @impl true
  def init({source, backend, ingest_ch_opts, query_ch_opts}) do
    resolve_timer = Process.send_after(self(), :resolve_connections, @resolve_interval)

    state = %__MODULE__{
      source: source,
      backend: backend,
      ingest_ch_opts: ingest_ch_opts,
      query_ch_opts: query_ch_opts,
      resolve_timer: resolve_timer
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:ensure_connection, connection_type}, _from, state) do
    {result, new_state} = ensure_connection_started(state, connection_type)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call({:connection_active?, connection_type}, _from, state) do
    active = connection_active?(state, connection_type)
    {:reply, active, state}
  end

  @impl true
  def handle_cast({:activity, :ingest}, state) do
    {:noreply, %{state | ingest_last_activity: System.system_time(:millisecond)}}
  end

  def handle_cast({:activity, :query}, state) do
    {:noreply, %{state | query_last_activity: System.system_time(:millisecond)}}
  end

  @impl true
  def handle_info(:resolve_connections, state) do
    if state.resolve_timer do
      Process.cancel_timer(state.resolve_timer)
    end

    new_timer = Process.send_after(self(), :resolve_connections, @resolve_interval)

    new_state =
      %__MODULE__{state | resolve_timer: new_timer}
      |> resolve_connection_state(:ingest)
      |> resolve_connection_state(:query)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) when is_pid(pid) do
    new_state =
      cond do
        pid == state.ingest_connection_pid ->
          Logger.warning("Clickhouse ingest connection proc died",
            source_id: state.source.id,
            backend_id: state.backend.id
          )

          %{state | ingest_connection_pid: nil}

        pid == state.query_connection_pid ->
          Logger.warning("Clickhouse query connection proc died",
            source_id: state.source.id,
            backend_id: state.backend.id
          )

          %{state | query_connection_pid: nil}

        true ->
          state
      end

    {:noreply, new_state}
  end

  defp ensure_connection_started(%__MODULE__{ingest_connection_pid: conn_pid} = state, :ingest)
       when is_pid(conn_pid) do
    if Process.alive?(conn_pid) do
      {:ok, record_activity(state, :ingest)}
    else
      start_connection(state, :ingest)
    end
  end

  defp ensure_connection_started(%__MODULE__{} = state, :ingest) do
    start_connection(state, :ingest)
  end

  defp ensure_connection_started(%__MODULE__{query_connection_pid: conn_pid} = state, :query)
       when is_pid(conn_pid) do
    if Process.alive?(conn_pid) do
      {:ok, record_activity(state, :query)}
    else
      start_connection(state, :query)
    end
  end

  defp ensure_connection_started(%__MODULE__{} = state, :query) do
    start_connection(state, :query)
  end

  defp connection_active?(%__MODULE__{ingest_connection_pid: pid}, :ingest) do
    is_pid(pid) && Process.alive?(pid)
  end

  defp connection_active?(%__MODULE__{query_connection_pid: pid}, :query) do
    is_pid(pid) && Process.alive?(pid)
  end

  defp record_activity(%__MODULE__{} = state, :ingest) do
    %{state | ingest_last_activity: System.system_time(:millisecond)}
  end

  defp record_activity(%__MODULE__{} = state, :query) do
    %{state | query_last_activity: System.system_time(:millisecond)}
  end

  defp start_connection(%__MODULE__{ingest_ch_opts: ingest_ch_opts} = state, :ingest) do
    case Ch.start_link(ingest_ch_opts) do
      {:ok, pid} ->
        Process.monitor(pid)

        Logger.info("Started Clickhouse ingest connection pool",
          source_id: state.source.id,
          backend_id: state.backend.id
        )

        new_state =
          %__MODULE__{state | ingest_connection_pid: pid}
          |> record_activity(:ingest)

        {:ok, new_state}

      {:error, reason} ->
        Logger.error("Failed to start Clickhouse ingest connection pool",
          source_id: state.source.id,
          backend_id: state.backend.id,
          reason: reason
        )

        {{:error, reason}, state}
    end
  end

  defp start_connection(%__MODULE__{query_ch_opts: query_ch_opts} = state, :query) do
    case Ch.start_link(query_ch_opts) do
      {:ok, pid} ->
        Process.monitor(pid)

        Logger.info("Started Clickhouse query connection pool",
          source_id: state.source.id,
          backend_id: state.backend.id
        )

        new_state =
          %__MODULE__{state | query_connection_pid: pid}
          |> record_activity(:query)

        {:ok, new_state}

      {:error, reason} ->
        Logger.error("Failed to start Clickhouse query connection pool",
          source_id: state.source.id,
          backend_id: state.backend.id,
          reason: reason
        )

        {{:error, reason}, state}
    end
  end

  defp resolve_connection_state(
         %__MODULE__{ingest_connection_pid: pid, ingest_last_activity: last_activity} = state,
         :ingest
       ) do
    now = System.system_time(:millisecond)

    cond do
      # No connection, no activity needed
      pid == nil ->
        state

      # Connection exists but not alive, clean up
      not Process.alive?(pid) ->
        %__MODULE__{state | ingest_connection_pid: nil}

      # No activity recorded yet, keep connection
      last_activity == nil ->
        state

      # Activity within timeout, keep connection
      now - last_activity < @ingest_inactivity_timeout ->
        state

      # Inactive for too long, stop connection
      true ->
        stop_connection(state, :ingest)
    end
  end

  defp resolve_connection_state(
         %__MODULE__{query_connection_pid: pid, query_last_activity: last_activity} = state,
         :query
       ) do
    now = System.system_time(:millisecond)

    cond do
      # No connection, no activity needed
      pid == nil ->
        state

      # Connection exists but not alive, clean up
      not Process.alive?(pid) ->
        %__MODULE__{state | query_connection_pid: nil}

      # No activity recorded yet, keep connection
      last_activity == nil ->
        state

      # Activity within timeout, keep connection
      now - last_activity < @query_inactivity_timeout ->
        state

      # Inactive for too long, stop connection
      true ->
        stop_connection(state, :query)
    end
  end

  defp stop_connection(%__MODULE__{ingest_connection_pid: pid} = state, :ingest) do
    if is_pid(pid) && Process.alive?(pid) do
      GenServer.stop(pid)

      Logger.info("Stopped Clickhouse ingest connection due to inactivity",
        source_id: state.source.id,
        backend_id: state.backend.id
      )
    end

    %__MODULE__{state | ingest_connection_pid: nil}
  end

  defp stop_connection(%__MODULE__{query_connection_pid: pid} = state, :query) do
    if is_pid(pid) && Process.alive?(pid) do
      GenServer.stop(pid)

      Logger.info("Stopped Clickhouse query connection due to inactivity",
        source_id: state.source.id,
        backend_id: state.backend.id
      )
    end

    %__MODULE__{state | query_connection_pid: nil}
  end

  defp stop_connection(%__MODULE__{} = state, _), do: state

  defp connection_manager_via({source, backend}) do
    Backends.via_source(source, __MODULE__, backend)
  end
end
