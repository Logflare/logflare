defmodule Logflare.Backends.SourceSupJanitor do
  @moduledoc """
  Performs cleanup of SourceSup that is idle.

  This GenServer monitors conditions and can switch between slow and fast checking modes.
  When in fast mode, it counts down checks and can trigger a shutdown function.
  """
  use GenServer
  require Logger

  alias Logflare.Source
  alias Logflare.Sources

  @slow_check_interval 5 * 60 * 1000  # 5 minutes
  @fast_check_interval 10_000         # 10 seconds

  @type state :: %{
    source_id: integer(),
    mode: :slow | :fast,
    slow_interval: pos_integer(),
    fast_interval: pos_integer(),
    remaining_fast_checks: nil | pos_integer()
  }

  @type option ::
    {:source, Source.t()} |
    {:slow_check_interval, pos_integer()} |
    {:fast_check_interval, pos_integer()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts ++ [sup_pid: self()])
  end

  @spec init([option()]) :: {:ok, state()}
  def init(opts) when is_list(opts) do
    source = Keyword.fetch!(opts, :source)
    slow_interval = Keyword.get(opts, :slow_check_interval, @slow_check_interval)
    fast_interval = Keyword.get(opts, :fast_check_interval, @fast_check_interval)

    Process.send_after(self(), :check, slow_interval)

    state = %{
      source_id: source.id,
      mode: :slow,
      slow_interval: slow_interval,
      fast_interval: fast_interval,
      remaining_fast_checks: nil,
      sup_pid: Keyword.fetch!(opts, :sup_pid)
    }

    {:ok, state}
  end

  @spec handle_info(:check, state()) :: {:noreply, state()}
  def handle_info(:check, %{mode: :slow} = state) do
    source = Sources.Cache.get_by_id(state.source_id)
    condition_met = check_condition(source)

    if condition_met do
      Logger.debug("SourceSupJanitor switching to fast mode for source #{source.name}")
      Process.send_after(self(), :check, state.fast_interval)
      {:noreply, %{state | mode: :fast, remaining_fast_checks: 6}}
    else
      Process.send_after(self(), :check, state.slow_interval)
      {:noreply, state}
    end
  end

  def handle_info(:check, %{mode: :fast, remaining_fast_checks: 1} = state) do
    source = Sources.Cache.get_by_id(state.source_id)
    condition_met = check_condition(source)
    Logger.debug("SourceSupJanitor final fast check for source #{source.name}: condition_met=#{condition_met}")

    if condition_met do
      Logger.info("SourceSupJanitor stopping idle source #{source.name}")
      Logflare.Source.Supervisor.stop_source_local(source)
      Process.send_after(self(), :check, state.slow_interval)
      {:noreply, %{state | mode: :slow, remaining_fast_checks: nil}}
    else
      Logger.debug("SourceSupJanitor condition no longer met, returning to slow mode for source #{source.name}")
      Process.send_after(self(), :check, state.slow_interval)
      {:noreply, %{state | mode: :slow, remaining_fast_checks: nil}}
    end
  end

  def handle_info(:check, %{mode: :fast} = state) do
    source = Sources.Cache.get_by_id(state.source_id)
    condition_met = check_condition(source)
    Logger.debug("SourceSupJanitor fast check for source #{source.name}: condition_met=#{condition_met}, remaining_checks=#{state.remaining_fast_checks}")

    if condition_met do
      Process.send_after(self(), :check, state.fast_interval)
      {:noreply, %{state | remaining_fast_checks: state.remaining_fast_checks - 1}}
    else
      Logger.debug("SourceSupJanitor condition no longer met, returning to slow mode for source #{source.name}")
      Process.send_after(self(), :check, state.slow_interval)
      {:noreply, %{state | mode: :slow, remaining_fast_checks: nil}}
    end
  end

  @spec check_condition(Source.t()) :: boolean()
  defp check_condition(%Source{} = source) do
    metrics = Sources.get_source_metrics_for_ingest(source)
    # Consider the source idle if average ingest rate is 0
    metrics.avg == 0
  end

end
