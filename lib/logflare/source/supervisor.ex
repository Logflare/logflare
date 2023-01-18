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

  def handle_continue(:boot, _source_ids) do
    # Starting sources by latest events first
    # Starting sources only when we've seen an event in the last 24 hours
    # Plugs.EnsureSourceStarted makes sure if a source isn't started, it gets started for ingest and the UI

    milli = :timer.hours(24)
    from_datetime = DateTime.utc_now() |> DateTime.add(-milli, :millisecond)

    query =
      from(s in Source,
        order_by: s.log_events_updated_at,
        where: s.log_events_updated_at > ^from_datetime,
        select: s
      )

    sources =
      query
      |> Repo.all()

    Enum.map(sources, fn source ->
      rls = %RLS{source_id: source.token, source: source}
      Supervisor.child_spec({RLS, rls}, id: source.token, restart: :transient)
    end)
    |> Enum.chunk_every(100)
    |> Enum.each(fn children ->
      # BigQuery Rate limit is 100/second
      # Also gives the database a break on boot
      # Logger.info("Sleeping for startup Logflare.Source.Supervisor")
      Process.sleep(250)
      Supervisor.start_link(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
    end)

    {:noreply, sources}
  end

  def handle_cast({:create, source_id}, state) do
    case Process.whereis(source_id) do
      nil ->
        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_cast({:delete, source_id}, state) do
    case Process.whereis(source_id) do
      nil ->
        {:noreply, state}

      _ ->
        send(source_id, {:stop_please, :shutdown})
        Counters.delete(source_id)

        state = List.delete(state, source_id)
        {:noreply, state}
    end
  end

  def handle_cast({:restart, source_id}, state) do
    case Process.whereis(source_id) do
      nil ->
        case create_source(source_id) do
          {:ok, _pid} ->
            state = Enum.uniq([source_id | state])
            {:noreply, state}

          {:error, _reason} ->
            Logger.error("Failed to start RecentLogsServer: #{source_id}")

            {:noreply, state}
        end

        {:noreply, state}

      _ ->
        send(source_id, {:stop_please, :shutdown})

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

  def start_source(source_id) when is_atom(source_id) do
    # Calling this server doing boot times out due to dealing with bigquery in init_table()

    GenServer.abcast(__MODULE__, {:create, source_id})

    {:ok, source_id}
  end

  def delete_source(source_id) do
    GenServer.abcast(__MODULE__, {:delete, source_id})
    BigQuery.delete_table(source_id)

    {:ok, source_id}
  end

  def reset_source(source_id) do
    GenServer.abcast(__MODULE__, {:restart, source_id})

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

  defp create_source(source_id) do
    # Double check source is in the database before starting
    # Can be removed when manager fns move into their own genserver

    source = Sources.get_by(token: source_id)

    if source do
      rls = %RLS{source_id: source_id, source: source}

      children = [
        Supervisor.child_spec({RLS, rls}, id: source_id, restart: :transient)
      ]

      init_table(source_id)

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

  def ensure_started(source_id) do
    case Process.whereis(source_id) do
      nil ->
        Logger.info("Source process not found, starting...", source_id: source_id)

        start_source(source_id)

        {:ok, :started}

      _else ->
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
