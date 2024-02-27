defmodule Logflare.Source.Supervisor do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """

  use GenServer

  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Google.BigQuery
  alias Logflare.Utils.Tasks
  alias Logflare.Source.V1SourceDynSup
  alias Logflare.Source.V1SourceSup
  alias Logflare.ContextCache
  alias Logflare.SourceSchemas

  import Ecto.Query, only: [from: 2]

  require Logger
  @agent __MODULE__.State

  # TODO: Move all manager fns into a manager server so errors in manager fns don't kill the whole supervision tree

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    Process.flag(:trap_exit, true)
    Agent.start_link(fn -> %{status: :boot} end, name: @agent)

    {:ok, nil, {:continue, :boot}}
  end

  ## Server

  def handle_continue(:boot, state) do
    # Starting sources by latest events first
    # Starting sources only when we've seen an event in the last 6 hours
    # Plugs.EnsureSourceStarted makes sure if a source isn't started, it gets started for ingest and the UI

    milli = :timer.hours(6)
    from_datetime = DateTime.utc_now() |> DateTime.add(-milli, :millisecond)

    query =
      from(s in Source,
        order_by: s.log_events_updated_at,
        where: s.log_events_updated_at > ^from_datetime,
        select: s,
        limit: 10_000
      )

    Repo.all(query)
    |> Enum.chunk_every(25)
    |> Enum.each(fn chunk ->
      for source <- chunk do
        rls = %RLS{source_id: source.token, source: source}
        DynamicSupervisor.start_child(V1SourceDynSup, {V1SourceSup, rls})
      end

      # BigQuery Rate limit is 100/second
      # Also gives the database a break on boot
      Process.sleep(250)
    end)

    Agent.update(@agent, &%{&1 | status: :ok})
    {:noreply, state}
  end

  def handle_cast({:create, source_token}, state) do
    with {:error, :no_proc} <- lookup(V1SourceSup, source_token),
         {:ok, _pid} <- create_source(source_token) do
      {:noreply, state}
    else
      {:ok, pid} when is_pid(pid) ->
        {:noreply, state}

      {:error, _reason} = err ->
        Logger.error(
          "Source.Supervisor -  Failed to start V1SourceSup: #{source_token}, #{inspect(err)}"
        )

        {:noreply, state}
    end
  end

  def handle_cast({:delete, source_token}, state) do
    case lookup(V1SourceSup, source_token) do
      {:error, _} ->
        {:noreply, state}

      {:ok, pid} ->
        DynamicSupervisor.terminate_child(V1SourceDynSup, pid)
        Counters.delete(source_token)
        {:noreply, state}
    end
  end

  def handle_cast({:restart, source_token}, state) do
    case lookup(V1SourceSup, source_token) do
      {:ok, pid} ->
        Logger.info(
          "Source.Supervisor - Performing V1SourceSup shutdown actions: #{source_token}"
        )

        DynamicSupervisor.terminate_child(V1SourceDynSup, pid)

        # perform context cache clearing
        source = Sources.get_source_by_token(source_token)
        source_schema = SourceSchemas.get_source_schema_by(source_id: source.id)

        ContextCache.bust_keys([
          {Sources, source.id},
          {SourceSchemas, source_schema.id}
        ])

      {:error, :no_proc} ->
        Logger.warning(
          "Source.Supervisor - V1SourceSup is not up. Attempting to start: #{source_token}"
        )

        :noop
    end

    case create_source(source_token) do
      {:ok, _pid} ->
        {:noreply, state}

      {:error, :already_started} ->
        Logger.info(
          "V1SourceSup already started by another concurrent action, will not attempt further start: #{source_token}"
        )

        {:noreply, state}

      {:error, _reason} = err ->
        Logger.error(
          "Failed to start V1SourceSup when attempting restart: #{source_token} , #{inspect(err)} "
        )

        {:noreply, state}
    end
  end

  def terminate(reason, state) do
    Logger.warning("Going Down - #{inspect(reason)} - #{__MODULE__} - last state: #{state}")
    reason
  end

  ## Public Functions

  @spec booting?() :: boolean()
  def booting?() do
    Agent.get(@agent, & &1)
    |> case do
      %{status: :ok} -> false
      _ -> true
    end
  end

  def start_source(source_token) when is_atom(source_token) do
    # Calling this server doing boot times out due to dealing with bigquery in init_table()
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:create, source_token})
    end

    {:ok, source_token}
  end

  def delete_source(source_token) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:delete, source_token})
      BigQuery.delete_table(source_token)
    end

    {:ok, source_token}
  end

  def reset_source(source_token) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:restart, source_token})
    end

    {:ok, source_token}
  end

  def delete_all_user_sources(user) do
    Repo.all(Ecto.assoc(user, :sources))
    |> Enum.each(fn s -> delete_source(s.token) end)
  end

  def reset_all_user_sources(user) do
    Repo.all(Ecto.assoc(user, :sources))
    |> Enum.each(fn s -> reset_source(s.token) end)
  end

  @doc """
  Returns the `:via` tuple for a given module for a source.
  """
  @spec via(module(), atom()) :: identifier()
  def via(module, source_token) when is_atom(source_token) do
    {:via, Registry, {Logflare.V1SourceRegistry, {module, source_token}, :registered}}
  end

  @doc """
  Looks up V1SourceRegistry for the provided module and source token.
  """
  @spec lookup(module(), atom()) :: {:ok, pid()} | {:error, :no_proc}
  def lookup(module, source_token) when is_atom(source_token) do
    case Registry.lookup(Logflare.V1SourceRegistry, {module, source_token}) do
      [{pid, :registered}] -> {:ok, pid}
      [] -> {:error, :no_proc}
    end
  end

  defp do_pg_ops?() do
    !!Application.get_env(:logflare, :single_tenant) &&
      !!Application.get_env(:logflare, :postgres_backend_adapter)
  end

  defp create_source(source_token) do
    # Double check source is in the database before starting
    # Can be removed when manager fns move into their own genserver
    source = Sources.get_by(token: source_token)

    if source do
      rls = %RLS{source_id: source_token, source: source}

      case DynamicSupervisor.start_child(V1SourceDynSup, {V1SourceSup, rls}) do
        {:ok, _pid} = res ->
          Tasks.start_child(fn -> init_table(source_token) end)

          res

        {:error, {:already_started = reason, _pid}} ->
          {:error, reason}
      end
    else
      {:error, :not_found_in_db}
    end
  end

  @spec ensure_started(atom) :: {:ok, :already_started | :started}
  def ensure_started(source_token) do
    case lookup(V1SourceSup, source_token) do
      {:error, _} ->
        Logger.info("Source.Supervisor - V1SourceSup not found, starting...",
          source_id: source_token,
          source_token: source_token
        )

        start_source(source_token)

        {:ok, :started}

      {:ok, _pid} ->
        {:ok, :already_started}
    end
  end

  def init_table(source_token) do
    %{
      user_id: user_id,
      bigquery_table_ttl: bigquery_table_ttl,
      bigquery_project_id: bigquery_project_id,
      bigquery_dataset_location: bigquery_dataset_location,
      bigquery_dataset_id: bigquery_dataset_id
    } = BigQuery.GenUtils.get_bq_user_info(source_token)

    BigQuery.init_table!(
      user_id,
      source_token,
      bigquery_project_id,
      bigquery_table_ttl,
      bigquery_dataset_location,
      bigquery_dataset_id
    )
  end
end
