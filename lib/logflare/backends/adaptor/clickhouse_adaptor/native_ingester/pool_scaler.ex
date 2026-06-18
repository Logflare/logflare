defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolScaler do
  @moduledoc """
  Dynamically scales the number of ClickHouse native TCP connection pools per backend.

  Runs one GenServer per backend. Watches checkout duration samples emitted by
  `NativeIngester` and decides whether to add or remove `Pool` instances under
  `PoolSup.PoolDynamicSupervisor`.

  ## Reactivity

  Scaling is driven by two complementary signals:

    * A sliding-window p95 of recent checkout wait time (steady-state)
    * An exponential moving average (EMA) of checkout wait time (fast-moving)

  A timeout sample triggers an **immediate** scale-up evaluation rather than
  waiting for the periodic tick. The scale step is **proportional** to how
  far the observed wait is above the threshold — small overshoot adds one
  pool, large overshoot can add several at once (clamped at `max_pool_count`).
  Symmetrically, scale-down steps are also proportional when traffic drops.

  Active pool indexes are stored in `:persistent_term` so that `pick_pool/1`
  is a lock-free O(1) read on the hot insert path.
  """

  use GenServer
  use TypedStruct

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup

  @sample_window 200
  # EMA smoothing factor (0 < alpha < 1). Higher = more reactive.
  @ema_alpha 0.3
  @pt_prefix :logflare_ch_pool_scaler_indexes

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :active_indexes, [non_neg_integer()], default: [0]
    field :wait_samples, [non_neg_integer()], default: []
    field :ema_wait_us, float(), default: 0.0
    field :timeout_count, non_neg_integer(), default: 0
    field :last_scale_up_at, integer() | nil, default: nil
    field :last_scale_down_at, integer() | nil, default: nil
    field :scale_timer_ref, reference() | nil, default: nil
  end

  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: via(backend))
  end

  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, backend.id},
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @spec via(Backend.t()) :: GenServer.name()
  def via(%Backend{} = backend) do
    Backends.via_backend(backend, __MODULE__)
  end

  @doc """
  Returns a pool index for the backend, distributing load across active pools.

  Reads from `:persistent_term` — O(1), no GenServer round-trip.
  Falls back to index 0 if no entry exists (scaler not yet started).
  """
  @spec pick_pool(Backend.t()) :: non_neg_integer()
  def pick_pool(%Backend{id: backend_id}) do
    indexes =
      try do
        :persistent_term.get(pt_key(backend_id))
      rescue
        ArgumentError -> [0]
      end

    case indexes do
      [single] ->
        single

      [_ | _] = many ->
        pos = rem(System.unique_integer([:positive]), length(many))
        Enum.at(many, pos)
    end
  end

  @doc """
  Records a checkout duration sample. Timeouts trigger an immediate scale check.
  """
  @spec record_sample(Backend.t(), non_neg_integer(), :ok | :timeout) :: :ok
  def record_sample(%Backend{} = backend, duration_us, result)
      when is_integer(duration_us) and result in [:ok, :timeout] do
    case GenServer.whereis(via(backend)) do
      nil -> :ok
      pid -> GenServer.cast(pid, {:record_sample, duration_us, result})
    end
  end

  @impl true
  def init(%Backend{id: backend_id} = backend) do
    config = ch_config()
    min_count = resolve_min_count(backend, config)
    interval = config[:pool_scale_interval_ms] || 3_000

    initial_indexes = Enum.to_list(0..(min_count - 1))
    :persistent_term.put(pt_key(backend_id), initial_indexes)

    timer_ref = Process.send_after(self(), :tick, interval)

    {:ok,
     %__MODULE__{
       backend_id: backend_id,
       active_indexes: initial_indexes,
       scale_timer_ref: timer_ref
     }}
  end

  @impl true
  def terminate(_reason, %__MODULE__{backend_id: backend_id}) do
    :persistent_term.erase(pt_key(backend_id))
    :ok
  end

  @impl true
  def handle_cast({:record_sample, duration_us, result}, %__MODULE__{} = state) do
    state =
      %{
        state
        | wait_samples: add_sample(state.wait_samples, duration_us),
          ema_wait_us: update_ema(state.ema_wait_us, duration_us),
          timeout_count: state.timeout_count + if(result == :timeout, do: 1, else: 0)
      }

    # Timeouts indicate immediate saturation — evaluate scale-up now.
    if result == :timeout do
      send(self(), :tick)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    if state.scale_timer_ref, do: Process.cancel_timer(state.scale_timer_ref)

    config = ch_config()
    new_state = state |> maybe_scale(config) |> schedule_next(config)
    {:noreply, new_state}
  end

  @spec maybe_scale(__MODULE__.t(), keyword()) :: __MODULE__.t()
  defp maybe_scale(%__MODULE__{} = state, config) do
    case Backends.Cache.get_backend(state.backend_id) do
      nil -> state
      backend -> do_scale(state, backend, config)
    end
  end

  @spec do_scale(__MODULE__.t(), Backend.t(), keyword()) :: __MODULE__.t()
  defp do_scale(%__MODULE__{} = state, %Backend{} = backend, app_config) do
    up_threshold_us = (app_config[:pool_scale_up_wait_ms] || 500) * 1_000
    down_threshold_us = (app_config[:pool_scale_down_wait_ms] || 50) * 1_000
    cooldown_ms = app_config[:pool_scale_cooldown_ms] || 10_000
    max_count = resolve_max_count(backend, app_config)
    min_count = resolve_min_count(backend, app_config)

    active_count = length(state.active_indexes)
    # Use the more reactive of EMA vs p95 — bursts surface in EMA first.
    p95_us = p95(state.wait_samples)
    signal_us = max(p95_us, trunc(state.ema_wait_us))
    now = System.monotonic_time(:millisecond)

    cond do
      scale_up?(signal_us, up_threshold_us, state.timeout_count, active_count, max_count) ->
        step = up_step(signal_us, up_threshold_us, state.timeout_count, active_count, max_count)
        apply_scale_up(state, backend, step, signal_us, now)

      scale_down?(signal_us, down_threshold_us, active_count, min_count, state.last_scale_down_at, now, cooldown_ms) ->
        step = down_step(signal_us, down_threshold_us, active_count, min_count)
        apply_scale_down(state, backend, step, signal_us, now)

      true ->
        # Keep samples between ticks so successive ticks see steady-state.
        # Clear only the burst flag.
        %{state | timeout_count: 0}
    end
  end

  @spec scale_up?(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), pos_integer()) ::
          boolean()
  defp scale_up?(signal_us, threshold_us, timeout_count, active_count, max_count) do
    active_count < max_count and (signal_us >= threshold_us or timeout_count > 0)
  end

  @spec up_step(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer(), pos_integer()) ::
          pos_integer()
  defp up_step(signal_us, threshold_us, timeout_count, active_count, max_count) do
    headroom = max_count - active_count

    pressure_step =
      cond do
        threshold_us <= 0 -> 1
        signal_us <= 0 -> 1
        true -> max(1, div(signal_us, threshold_us))
      end

    # Timeouts amplify urgency.
    burst_step = if timeout_count > 0, do: max(2, timeout_count), else: 1

    pressure_step
    |> max(burst_step)
    |> min(headroom)
  end

  @spec scale_down?(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          integer() | nil,
          integer(),
          non_neg_integer()
        ) :: boolean()
  defp scale_down?(signal_us, threshold_us, active_count, min_count, last_down_at, now, cooldown_ms) do
    cooldown_passed = is_nil(last_down_at) or now - last_down_at >= cooldown_ms

    active_count > min_count and signal_us <= threshold_us and cooldown_passed
  end

  @spec down_step(non_neg_integer(), non_neg_integer(), non_neg_integer(), pos_integer()) ::
          pos_integer()
  defp down_step(signal_us, threshold_us, active_count, min_count) do
    headroom = active_count - min_count

    # If signal is far below threshold, shed more pools at once (up to half active).
    aggressiveness =
      cond do
        threshold_us <= 0 -> 1
        signal_us <= div(threshold_us, 4) -> max(1, div(active_count, 2))
        signal_us <= div(threshold_us, 2) -> 2
        true -> 1
      end

    min(headroom, aggressiveness)
  end

  @spec apply_scale_up(__MODULE__.t(), Backend.t(), pos_integer(), non_neg_integer(), integer()) ::
          __MODULE__.t()
  defp apply_scale_up(state, backend, step, signal_us, now) do
    {new_indexes, added} =
      Enum.reduce(1..step, {state.active_indexes, 0}, fn _, {indexes, added} ->
        next_index = length(indexes)

        case PoolSup.start_pool(backend, next_index) do
          :ok -> {indexes ++ [next_index], added + 1}
          {:error, _} -> {indexes, added}
        end
      end)

    if added > 0 do
      :persistent_term.put(pt_key(state.backend_id), new_indexes)

      Logger.info("ClickHouse native pool scaled up",
        backend_id: state.backend_id,
        active_pools: length(new_indexes),
        step: added,
        signal_us: signal_us
      )

      emit_scale_telemetry(state.backend_id, :up, length(new_indexes), added, signal_us)

      %{
        state
        | active_indexes: new_indexes,
          timeout_count: 0,
          last_scale_up_at: now,
          wait_samples: [],
          ema_wait_us: 0.0
      }
    else
      %{state | timeout_count: 0}
    end
  end

  @spec apply_scale_down(__MODULE__.t(), Backend.t(), pos_integer(), non_neg_integer(), integer()) ::
          __MODULE__.t()
  defp apply_scale_down(state, backend, step, signal_us, now) do
    # Enum.split(list, -step) keeps the prefix and returns the last `step` as dropped.
    {kept, dropped} = Enum.split(state.active_indexes, -step)

    Enum.each(dropped, fn idx -> PoolSup.stop_pool(backend, idx) end)

    :persistent_term.put(pt_key(state.backend_id), kept)

    Logger.info("ClickHouse native pool scaled down",
      backend_id: state.backend_id,
      active_pools: length(kept),
      step: length(dropped),
      signal_us: signal_us
    )

    emit_scale_telemetry(state.backend_id, :down, length(kept), length(dropped), signal_us)

    %{
      state
      | active_indexes: kept,
        last_scale_down_at: now,
        wait_samples: [],
        ema_wait_us: 0.0
    }
  end

  @spec emit_scale_telemetry(pos_integer(), :up | :down, non_neg_integer(), pos_integer(), non_neg_integer()) ::
          :ok
  defp emit_scale_telemetry(backend_id, direction, active_pools, step, signal_us) do
    :telemetry.execute(
      [:logflare, :backends, :clickhouse, :pool, :scale],
      %{active_pools: active_pools, step: step, signal_us: signal_us},
      %{backend_id: backend_id, direction: direction}
    )
  end

  @spec schedule_next(__MODULE__.t(), keyword()) :: __MODULE__.t()
  defp schedule_next(%__MODULE__{} = state, config) do
    interval = config[:pool_scale_interval_ms] || 3_000
    timer_ref = Process.send_after(self(), :tick, interval)
    %{state | scale_timer_ref: timer_ref}
  end

  @spec add_sample([non_neg_integer()], non_neg_integer()) :: [non_neg_integer()]
  defp add_sample(samples, value) do
    [value | Enum.take(samples, @sample_window - 1)]
  end

  @spec update_ema(float(), non_neg_integer()) :: float()
  defp update_ema(0.0, value), do: value * 1.0
  defp update_ema(prev, value), do: @ema_alpha * value + (1.0 - @ema_alpha) * prev

  @spec p95([non_neg_integer()]) :: non_neg_integer()
  defp p95([]), do: 0

  defp p95(samples) do
    sorted = Enum.sort(samples)
    idx = min(trunc(length(sorted) * 0.95), length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  @spec resolve_min_count(Backend.t(), keyword()) :: pos_integer()
  defp resolve_min_count(%Backend{config: cfg}, app_config) do
    Map.get(cfg, :min_pool_count) || app_config[:min_pool_count] || 1
  end

  @spec resolve_max_count(Backend.t(), keyword()) :: pos_integer()
  defp resolve_max_count(%Backend{config: cfg}, app_config) do
    Map.get(cfg, :max_pool_count) || app_config[:max_pool_count] || 4
  end

  @spec pt_key(pos_integer()) :: {atom(), pos_integer()}
  defp pt_key(backend_id), do: {@pt_prefix, backend_id}

  @spec ch_config() :: keyword()
  defp ch_config do
    Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)
  end
end
