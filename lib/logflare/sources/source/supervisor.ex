defmodule Logflare.Sources.Source.Supervisor do
  @moduledoc """
  Boots up a gen server per source table. Keeps a list of active tables in state.
  """
  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.SystemBackend
  alias Logflare.ContextCache
  alias Logflare.Google.BigQuery
  alias Logflare.Repo
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Counters
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
    stop_source_local(source)
    Counters.delete(source_token)
    {:noreply, state}
  end

  def handle_cast({:restart, source_token}, state) do
    source = Sources.get_source_by_token(source_token)

    stop_source_local(source)
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
    GenServer.abcast(__MODULE__, {:create, source_token})

    {:ok, source_token}
  end

  def delete_source(source_token) do
    GenServer.abcast(__MODULE__, {:stop, source_token})
    # TODO: move to adaptor callback
    if Backends.bigquery_default_backend?() do
      BigQuery.delete_table(source_token)
    end

    {:ok, source_token}
  end

  def stop_source(source_token) do
    GenServer.abcast(__MODULE__, {:stop, source_token})
    {:ok, source_token}
  end

  def reset_source(source_token) do
    if Backends.bigquery_default_backend?() do
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

  defp create_source(%Source{} = source) do
    with {:ok, _pid} = res <- do_start_source_sup(source),
         :ok <- SystemBackend.on_source_start(source) do
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

  @spec ensure_started(Source.t()) :: :ok
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

  defp do_start_source_sup(source) do
    with :ok <- Backends.start_source_sup(source) do
      do_lookup(source)
    end
  end

  defp do_lookup(source), do: Backends.lookup(Backends.SourceSup, source)

  def stop_source_local(%Source{} = source) do
    with {:ok, pid} <- do_lookup(source) do
      Logflare.Utils.try_to_stop_process(pid, :shutdown)
    end

    :ok
  end
end
