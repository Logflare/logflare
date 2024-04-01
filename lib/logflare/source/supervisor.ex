defmodule Logflare.Source.Supervisor do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """

  use GenServer

  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.Utils.Tasks
  alias Logflare.Source.V1SourceDynSup
  alias Logflare.Source.V1SourceSup
  alias Logflare.ContextCache
  alias Logflare.SourceSchemas
  alias Logflare.Backends

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
        do_start_source_sup(source)
      end

      # BigQuery Rate limit is 100/second
      # Also gives the database a break on boot
      Process.sleep(250)
    end)

    Agent.update(@agent, &%{&1 | status: :ok})
    {:noreply, state}
  end

  def handle_cast({:create, source_token}, state) do
    source = Sources.Cache.get_by(token: source_token)

    case create_source(source) do
      {:error, :already_started} ->
        :noop

      {:error, _} = err ->
        Logger.error(
          "Source.Supervisor -  Failed to start SourceSup: #{source_token}, #{inspect(err)}"
        )

      _ ->
        :noop
    end

    {:noreply, state}
  end

  def handle_cast({:stop, source_token}, state) do
    source = Sources.Cache.get_by(token: source_token)
    do_terminate_source_sup(source)
    Counters.delete(source_token)
    {:noreply, state}
  end

  def handle_cast({:restart, source_token}, state) do
    source = Sources.get_source_by_token(source_token)

    do_terminate_source_sup(source)
    source_schema = SourceSchemas.get_source_schema_by(source_id: source.id)

    ContextCache.bust_keys([
      {Sources, source.id},
      {SourceSchemas, source_schema.id}
    ])

    case create_source(source) do
      {:ok, _pid} ->
        :noop

      {:error, :already_started} ->
        Logger.info(
          "SourceSup already started by another concurrent action, will not attempt further start: #{source_token}"
        )

        :noop

      {:error, _reason} = err ->
        Logger.error(
          "Failed to start SourceSup when attempting restart: #{source_token} , #{inspect(err)} "
        )

        :noop
    end

    {:noreply, state}
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
    GenServer.abcast(__MODULE__, {:create, source_token})

    {:ok, source_token}
  end

  def delete_source(source_token) do
    GenServer.abcast(__MODULE__, {:stop, source_token})
    # TODO: move to adaptor callback
    unless do_pg_ops?() do
      BigQuery.delete_table(source_token)
    end

    {:ok, source_token}
  end

  def stop_source(source_token) do
    GenServer.abcast(__MODULE__, {:stop, source_token})
    {:ok, source_token}
  end

  def reset_source(source_token) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:restart, source_token})
    end

    {:ok, source_token}
  end

  def delete_all_user_sources(user) do
    # TODO: use context func
    Repo.all(Ecto.assoc(user, :sources))
    |> Enum.each(fn s -> delete_source(s.token) end)
  end

  def reset_all_user_sources(user) do
    # TODO: use context func
    Repo.all(Ecto.assoc(user, :sources))
    |> Enum.each(fn s -> reset_source(s.token) end)
  end

  defp do_pg_ops?() do
    !!Application.get_env(:logflare, :single_tenant) &&
      !!Application.get_env(:logflare, :postgres_backend_adapter)
  end

  defp create_source(%Source{} = source) do
    with {:ok, _pid} = res <- do_start_source_sup(source),
         :ok <- init_table(source.token) do
      res
    else
      {:error, :already_started} = err ->
        err

      {:error, {:already_started = reason, _pid}} ->
        {:error, reason}
    end
  end

  @spec ensure_started(atom) :: {:ok, :already_started | :started}
  def ensure_started(%Source{token: source_token, v2_pipeline: v2_pipeline} = source) do
    # maybe restart
    [{:v1, do_v1_lookup(source)}, {:v2, do_v2_lookup(source)}]
    |> Enum.each(fn
      {:v1, {:ok, _}} when v2_pipeline == true ->
        # v2->v1, restart the source
        reset_source(source_token)

      {:v2, {:ok, _}} when v2_pipeline == false ->
        # v1->v2 , restart the source
        reset_source(source_token)

      {ver, {:error, _}}
      when (ver == :v1 and v2_pipeline == false) or (ver == :v2 and v2_pipeline == true) ->
        Logger.info("Source.Supervisor - SourceSup not found, starting...",
          source_id: source_token,
          source_token: source_token
        )

        start_source(source_token)

      _ ->
        :noop
    end)

    :ok
  end

  def init_table(source_token) do
    %{
      user_id: user_id,
      bigquery_table_ttl: bigquery_table_ttl,
      bigquery_project_id: bigquery_project_id,
      bigquery_dataset_location: bigquery_dataset_location,
      bigquery_dataset_id: bigquery_dataset_id
    } = BigQuery.GenUtils.get_bq_user_info(source_token)

    Tasks.start_child(fn ->
      BigQuery.init_table!(
        user_id,
        source_token,
        bigquery_project_id,
        bigquery_table_ttl,
        bigquery_dataset_location,
        bigquery_dataset_id
      )
    end)

    :ok
  end

  defp do_start_source_sup(%{v2_pipeline: true} = source) do
    with :ok <- Backends.start_source_sup(source) do
      do_lookup(source)
    end
  end

  defp do_start_source_sup(source) do
    DynamicSupervisor.start_child(V1SourceDynSup, {V1SourceSup, source: source})
  end

  defp do_lookup(%{v2_pipeline: true} = source),
    do: do_v2_lookup(source)

  defp do_lookup(source), do: do_v1_lookup(source)
  defp do_v2_lookup(source), do: Backends.lookup(Backends.SourceSup, source)
  defp do_v1_lookup(source), do: Backends.lookup(V1SourceSup, source)

  defp do_terminate_source_sup(source) do
    with {:ok, pid} <- do_v2_lookup(source) do
      DynamicSupervisor.terminate_child(Backends.SourcesSup, pid)
    end

    with {:ok, pid} <- do_v1_lookup(source) do
      DynamicSupervisor.terminate_child(V1SourceDynSup, pid)
    end

    :ok
  end
end
