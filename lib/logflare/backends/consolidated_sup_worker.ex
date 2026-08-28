defmodule Logflare.Backends.ConsolidatedSupWorker do
  @moduledoc """
  Worker that performs periodic reconciliation for consolidated backend pipelines.

  Ensures that:
  - Pipelines are started for all backends that support consolidated ingestion
  - Stale pipelines are stopped when backends are deleted or no longer support consolidation
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.ConsolidatedSup

  @default_interval 30_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    state = %{interval: Keyword.get(opts, :interval, @default_interval)}
    Process.send_after(self(), :check, state.interval)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:check, state) do
    do_check()
    Process.send_after(self(), :check, state.interval)
    {:noreply, state}
  end

  defp do_check do
    expected_backend_ids = list_expected_backend_ids()
    running_backend_ids = list_running_backend_ids()

    for backend_id <- expected_backend_ids,
        backend_id not in running_backend_ids do
      start_pipeline(backend_id)
    end

    for backend_id <- running_backend_ids,
        backend_id not in expected_backend_ids do
      stop_pipeline(backend_id)
    end
  end

  defp list_expected_backend_ids do
    Backends.list_backends(has_sources_or_rules: true)
    |> Enum.filter(&Adaptor.consolidated_ingest?/1)
    |> Enum.map(& &1.id)
    |> MapSet.new()
  end

  defp list_running_backend_ids do
    ConsolidatedSup.list_pipelines()
    |> Enum.map(fn {backend_id, _pid} -> backend_id end)
    |> MapSet.new()
  end

  defp start_pipeline(backend_id) do
    case Backends.Cache.get_backend(backend_id) do
      nil ->
        Logger.warning("Cannot start consolidated pipeline: backend not found",
          backend_id: backend_id
        )

      backend ->
        case ConsolidatedSup.start_pipeline(backend) do
          {:ok, _pid} ->
            Logger.info("Started consolidated pipeline", backend_id: backend_id)

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, reason} ->
            Logger.warning("Failed to start consolidated pipeline",
              backend_id: backend_id,
              reason: inspect(reason)
            )
        end
    end
  end

  defp stop_pipeline(backend_id) do
    reason =
      if Backends.Cache.get_backend(backend_id),
        do: "no longer supports consolidated ingest",
        else: "backend no longer exists"

    Logger.info("Stopping consolidated pipeline: #{reason}", backend_id: backend_id)
    ConsolidatedSup.stop_pipeline(backend_id)
  end
end
