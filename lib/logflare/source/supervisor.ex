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
  alias Logflare.Source.V1SourceDynSup
  alias Logflare.Source.V1SourceSup
  alias Logflare.ContextCache
  alias Logflare.SourceSchemas
  alias Logflare.Backends
  alias Logflare.Utils.Tasks

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
      {Sources, source.id},
      {SourceSchemas, source_schema.id}
    ])

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

  def handle_cast({:maybe_restart_mismatched_source_pipelines, source_token}, state) do
    source = Sources.Cache.get_source_by_token(source_token)

    %{
      v1: do_v1_lookup(source),
      v2: do_v2_lookup(source),
      v2_pipeline: source.v2_pipeline
    }
    |> case do
      %{v1: {:ok, _}, v2_pipeline: true} ->
        # v2->v1, restart the source pipelines
        reset_source(source_token)

      %{v2: {:ok, _}, v2_pipeline: false} ->
        # v1->v2 , restart the source pipelines
        reset_source(source_token)

      _ ->
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

  def maybe_restart_mismatched_source_pipelines(source_token) do
    unless do_pg_ops?() do
      GenServer.abcast(__MODULE__, {:maybe_restart_mismatched_source_pipelines, source_token})
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

      {:error} = err ->
        err
    end
  end

  @spec ensure_started(atom) :: {:ok, :already_started | :started}
  def ensure_started(%Source{token: source_token, v2_pipeline: v2_pipeline} = source) do
    # maybe restart
    %{
      v1: do_v1_lookup(source),
      v2: do_v2_lookup(source),
      v2_pipeline: v2_pipeline
    }
    |> case do
      %{v1: {:ok, _}} when v2_pipeline == true ->
        # v2->v1, restart the source pipelines
        maybe_restart_mismatched_source_pipelines(source_token)

      %{v2: {:ok, _}} when v2_pipeline == false ->
        # v1->v2 , restart the source pipelines
        maybe_restart_mismatched_source_pipelines(source_token)

      %{v1: {:error, _}, v2: {:error, _}} ->
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

  defp do_start_source_sup(%{v2_pipeline: true} = source) do
    with :ok <- Backends.start_source_sup(source) do
      do_lookup(source)
    end
  end

  defp do_start_source_sup(source) do
    DynamicSupervisor.start_child(V1SourceDynSup, {V1SourceSup, source: source})
  end

  defp do_lookup(%Source{v2_pipeline: true} = source), do: do_v2_lookup(source)
  defp do_lookup(%Source{v2_pipeline: false} = source), do: do_v1_lookup(source)

  defp do_v2_lookup(source), do: Backends.lookup(Backends.SourceSup, source)
  defp do_v1_lookup(source), do: Backends.lookup(V1SourceSup, source)

  defp do_terminate_source_sup(%Source{} = source) do
    with {:ok, pid} <- do_v2_lookup(source) do
      DynamicSupervisor.terminate_child(
        {:via, PartitionSupervisor, {Backends.SourcesSup, source.id}},
        pid
      )
    end

    with {:ok, pid} <- do_v1_lookup(source) do
      DynamicSupervisor.terminate_child(V1SourceDynSup, pid)
    end

    :ok
  end
end
