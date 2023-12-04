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
  alias Logflare.SourceSchemas
  alias Logflare.Google.BigQuery
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Utils.Tasks

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
      children =
        for source <- chunk do
          rls = %RLS{source_id: source.token, source: source}
          Supervisor.child_spec({RLS, rls}, id: source.token, restart: :transient)
        end

      # BigQuery Rate limit is 100/second
      # Also gives the database a break on boot
      Process.sleep(250)

      Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
    end)

    Agent.update(@agent, &%{&1 | status: :ok})
    {:noreply, state}
  end

  def handle_cast({:create, source_token}, state) do
    case lookup(RLS, source_token) do
      {:error, _} ->
        case create_source(source_token) do
          {:ok, _pid} ->
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_token}")
            {:noreply, state}
        end

      {:ok, _pid} ->
        {:noreply, state}
    end
  end

  def handle_cast({:delete, source_token}, state) do
    case lookup(RLS, source_token) do
      {:error, _} ->
        {:noreply, state}

      {:ok, pid} ->
        send(pid, {:stop_please, :shutdown})
        Counters.delete(source_token)
        {:noreply, state}
    end
  end

  def handle_cast({:restart, source_token}, state) do
    case lookup(RLS, source_token) do
      {:error, _} ->
        case create_source(source_token) do
          {:ok, _pid} ->
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_token}")
            {:noreply, state}
        end

        {:noreply, state}

      {:ok, pid} ->
        send(pid, {:stop_please, :shutdown})

        reset_persisted_schema(source_token)

        Process.sleep(1_000)

        case create_source(source_token) do
          {:ok, _pid} ->
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_token}")

            {:noreply, state}
        end
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

      children = [Supervisor.child_spec({RLS, rls}, id: source_token, restart: :transient)]

      # fire off async init in async task, so that bq call does not block.
      Tasks.start_child(fn -> init_table(source_token) end)

      Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
    else
      {:error, :not_found_in_db}
    end
  end

  defp reset_persisted_schema(source_token) do
    # Resets our schema so then it'll get merged with BigQuery's when the next log event comes in for a source
    case Sources.get_by(token: source_token) do
      nil ->
        :noop

      source ->
        case SourceSchemas.get_source_schema_by(source_id: source.id) do
          nil ->
            :noop

          schema ->
            init_schema = SchemaBuilder.initial_table_schema()

            SourceSchemas.update_source_schema(schema, %{
              bigquery_schema: init_schema,
              schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(init_schema)
            })
        end
    end
  end

  @spec ensure_started(atom) :: {:ok, :already_started | :started}
  def ensure_started(source_token) do
    case lookup(RLS, source_token) do
      {:error, _} ->
        Logger.info("Source process not found, starting...", source_id: source_token)

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
