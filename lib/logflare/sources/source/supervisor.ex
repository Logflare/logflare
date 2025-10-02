defmodule Logflare.Sources.Source.Supervisor do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """
  use GenServer

  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.Google.BigQuery
  alias Logflare.Repo
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Google.BigQuery
  alias Logflare.ContextCache
  alias Logflare.SourceSchemas
  alias Logflare.Backends
  alias Logflare.Utils.Tasks
  alias Logflare.Sources.Source
  require Logger

  # TODO: Move all manager fns into a manager server so errors in manager fns don't kill the whole supervision tree

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    Process.flag(:trap_exit, true)

    {:ok, nil}
  end

  ## Server

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
      {Sources, source.id}
    ])

    if source_schema do
      ContextCache.bust_keys([{SourceSchemas, source_schema.id}])
    end

    case create_source(source) do
      {:ok, _pid} ->
        :noop

      {:error, :already_started} ->
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

  defp do_pg_ops? do
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

      {:error} = err ->
        err
    end
  end

  @spec ensure_started(atom) :: {:ok, :already_started | :started}
  def ensure_started(%Source{token: source_token} = source) do
    # Check if already running
    do_lookup(source)
    |> case do
      {:error, _} ->
        start_source(source_token)

      _ ->
        :noop
    end

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

  defp do_start_source_sup(source) do
    with :ok <- Backends.start_source_sup(source) do
      do_lookup(source)
    end
  end

  defp do_lookup(source), do: Backends.lookup(Backends.SourceSup, source)

  defp do_terminate_source_sup(%Source{} = source) do
    with {:ok, pid} <- do_lookup(source) do
      DynamicSupervisor.terminate_child(
        {:via, PartitionSupervisor, {Backends.SourcesSup, source.id}},
        pid
      )
    end

    :ok
  end
end
