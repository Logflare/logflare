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

  # TODO: Move all manager fns into a manager server so errors in manager fns don't kill the whole supervision tree

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(source_ids) do
    Process.flag(:trap_exit, true)

    {:ok, source_ids, {:continue, :boot}}
  end

  ## Server

  def handle_continue(:boot, source_ids) do
    # Starting sources by latest events first
    # Starting sources only when we've seen an event in the last 6 hours
    # Plugs.EnsureSourceStarted makes sure if a source isn't started, it gets started for ingest and the UI

    milli = :timer.hours(6)
    from_datetime = DateTime.utc_now() |> DateTime.add(-milli, :millisecond)

    query =
      from(s in Source,
        order_by: s.log_events_updated_at,
        where: s.log_events_updated_at > ^from_datetime,
        select: s
      )

    stream = Repo.stream(query, max_rows: 10_000)

    Repo.transaction(fn ->
      stream
      |> Stream.map(fn source ->
        rls = %RLS{source_id: source.token, source: source}
        Supervisor.child_spec({RLS, rls}, id: source.token, restart: :transient)
      end)
      |> Stream.chunk_every(25)
      |> Stream.each(fn children ->
        # BigQuery Rate limit is 100/second
        # Also gives the database a break on boot
        Process.sleep(250)
        Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
      end)
      |> Enum.to_list()
    end)

    {:noreply, source_ids}
  end

  def handle_cast({:create, source_id}, state) do
    case lookup(RLS, source_id) do
      {:error, _} ->
        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end

      {:ok, _pid} ->
        {:noreply, state}
    end
  end

  def handle_cast({:delete, source_id}, state) do
    case lookup(RLS, source_id) do
      {:error, _} ->
        {:noreply, state}

      {:ok, pid} ->
        send(pid, {:stop_please, :shutdown})
        Counters.delete(source_id)

        state = List.delete(state, source_id)
        {:noreply, state}
    end
  end

  def handle_cast({:restart, source_id}, state) do
    case lookup(RLS, source_id) do
      {:error, _} ->
        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end

        {:noreply, state}

      {:ok, pid} ->
        send(pid, {:stop_please, :shutdown})

        reset_persisted_schema(source_id)

        Process.sleep(1_000)

        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end
    end
  end

  def terminate(reason, _state) do
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}")
    reason
  end

  ## Public Functions

  def start_source(source_token) when is_atom(source_token) do
    # Calling this server doing boot times out due to dealing with bigquery in init_table()
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:create, source_token})
    end

    {:ok, source_token}
  end

  def delete_source(source_id) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:delete, source_id})
      BigQuery.delete_table(source_id)
    end

    {:ok, source_id}
  end

  def reset_source(source_id) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:restart, source_id})
    end

    {:ok, source_id}
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

  defp create_source(source_id) do
    # Double check source is in the database before starting
    # Can be removed when manager fns move into their own genserver
    source = Sources.get_by(token: source_id)

    if source do
      rls = %RLS{source_id: source_id, source: source}

      children = [Supervisor.child_spec({RLS, rls}, id: source_id, restart: :transient)]

      # fire off async init in async task, so that bq call does not block.
      Tasks.start_child(fn -> init_table(source_id) end)

      Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
    else
      {:error, :not_found_in_db}
    end
  end

  defp reset_persisted_schema(source_id) do
    # Resets our schema so then it'll get merged with BigQuery's when the next log event comes in for a source
    case Sources.get_by(token: source_id) do
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
  def ensure_started(source_id) do
    case lookup(RLS, source_id) do
      {:error, _} ->
        Logger.info("Source process not found, starting...", source_id: source_id)

        start_source(source_id)

        {:ok, :started}

      {:ok, _pid} ->
        {:ok, :already_started}
    end
  end

  def init_table(source_id) do
    %{
      user_id: user_id,
      bigquery_table_ttl: bigquery_table_ttl,
      bigquery_project_id: bigquery_project_id,
      bigquery_dataset_location: bigquery_dataset_location,
      bigquery_dataset_id: bigquery_dataset_id
    } = BigQuery.GenUtils.get_bq_user_info(source_id)

    BigQuery.init_table!(
      user_id,
      source_id,
      bigquery_project_id,
      bigquery_table_ttl,
      bigquery_dataset_location,
      bigquery_dataset_id
    )
  end
end
