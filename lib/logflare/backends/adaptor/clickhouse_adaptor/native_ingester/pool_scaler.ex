defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolScaler do
  @moduledoc """
  Dynamically scales the number of ClickHouse native TCP connection pools per backend.

  Runs one GenServer per backend. Listens to checkout duration telemetry emitted by
  `NativeIngester` and periodically decides whether to add or remove `Pool` instances
  under `PoolSup.PoolDynamicSupervisor`.

  Pool 0 is always managed by `PoolManager` (inactivity timeout). This scaler
  manages pools at index ≥ 1, expanding up to `max_pool_count` and contracting
  back to 1 as load subsides.

  Active pool indexes are stored in `:persistent_term` so that `pick_pool/1` is
  a cheap non-blocking read on the hot insert path.
  """

  use GenServer
  use TypedStruct

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup

  @sample_window 200
  @pt_prefix :logflare_ch_pool_scaler_indexes

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :active_indexes, [non_neg_integer()], default: [0]
    field :wait_samples, [non_neg_integer()], default: []
    field :timeout_count, non_neg_integer(), default: 0
    field :last_scale_down_at, integer() | nil, default: nil
    field :scale_timer_ref, reference() | nil, default: nil
  end

  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{id: backend_id} = backend) do
    GenServer.start_link(__MODULE__, backend_id, name: via(backend))
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

      [_ | _] ->
        count = length(indexes)
        pos = rem(System.unique_integer([:positive]), count)
        Enum.at(indexes, pos)
    end
  end

  @doc """
  Records a checkout duration sample for scaling decisions.

  Called from `NativeIngester` after each pool checkout attempt.
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
  def init(backend_id) do
    config = ch_config()
    min_pool_count = config[:min_pool_count] || 1
    interval = config[:pool_scale_interval_ms] || 10_000

    initial_indexes = Enum.to_list(0..(min_pool_count - 1))
    :persistent_term.put(pt_key(backend_id), initial_indexes)

    timer_ref = Process.send_after(self(), :scale, interval)

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
  def handle_cast({:record_sample, duration_us, :timeout}, %__MODULE__{} = state) do
    {:noreply,
     %{
       state
       | wait_samples: add_sample(state.wait_samples, duration_us),
         timeout_count: state.timeout_count + 1
     }}
  end

  def handle_cast({:record_sample, duration_us, _result}, %__MODULE__{} = state) do
    {:noreply, %{state | wait_samples: add_sample(state.wait_samples, duration_us)}}
  end

  @impl true
  def handle_info(:scale, %__MODULE__{} = state) do
    if state.scale_timer_ref, do: Process.cancel_timer(state.scale_timer_ref)

    config = ch_config()
    new_state = state |> maybe_scale(config) |> reset_samples() |> schedule_next(config)
    {:noreply, new_state}
  end

  @spec maybe_scale(__MODULE__.t(), keyword()) :: __MODULE__.t()
  defp maybe_scale(%__MODULE__{} = state, config) do
    backend = Backends.Cache.get_backend(state.backend_id)
    if is_nil(backend), do: state, else: do_scale(state, backend, config)
  end

  @spec do_scale(__MODULE__.t(), Backend.t(), keyword()) :: __MODULE__.t()
  defp do_scale(%__MODULE__{} = state, %Backend{config: backend_config} = backend, app_config) do
    up_threshold_us = (app_config[:pool_scale_up_wait_ms] || 500) * 1_000
    down_threshold_us = (app_config[:pool_scale_down_wait_ms] || 50) * 1_000
    cooldown_ms = app_config[:pool_scale_cooldown_ms] || 30_000
    max_count = Map.get(backend_config, :max_pool_count) || app_config[:max_pool_count] || 4
    min_count = Map.get(backend_config, :min_pool_count) || app_config[:min_pool_count] || 1

    active_count = length(state.active_indexes)
    p95_us = p95(state.wait_samples)
    now = System.monotonic_time(:millisecond)

    should_scale_up =
      (p95_us >= up_threshold_us or state.timeout_count > 0) and active_count < max_count

    since_last_down = if state.last_scale_down_at, do: now - state.last_scale_down_at, else: nil

    should_scale_down =
      p95_us <= down_threshold_us and active_count > min_count and
        (is_nil(since_last_down) or since_last_down >= cooldown_ms)

    cond do
      should_scale_up ->
        next_index = active_count

        case PoolSup.start_pool(backend, next_index) do
          :ok ->
            new_indexes = state.active_indexes ++ [next_index]

            Logger.info("ClickHouse native pool scaled up",
              backend_id: state.backend_id,
              active_pools: length(new_indexes),
              p95_wait_ms: div(p95_us, 1_000)
            )

            emit_scale_telemetry(state.backend_id, length(new_indexes), p95_us)
            :persistent_term.put(pt_key(state.backend_id), new_indexes)
            %{state | active_indexes: new_indexes, timeout_count: 0}

          {:error, _} ->
            state
        end

      should_scale_down ->
        last_index = List.last(state.active_indexes)
        new_indexes = List.delete(state.active_indexes, last_index)

        PoolSup.stop_pool(backend, last_index)

        Logger.info("ClickHouse native pool scaled down",
          backend_id: state.backend_id,
          active_pools: length(new_indexes),
          p95_wait_ms: div(p95_us, 1_000)
        )

        emit_scale_telemetry(state.backend_id, length(new_indexes), p95_us)
        :persistent_term.put(pt_key(state.backend_id), new_indexes)
        %{state | active_indexes: new_indexes, last_scale_down_at: now}

      true ->
        state
    end
  end

  @spec emit_scale_telemetry(pos_integer(), non_neg_integer(), non_neg_integer()) :: :ok
  defp emit_scale_telemetry(backend_id, active_pools, p95_wait_us) do
    :telemetry.execute(
      [:logflare, :backends, :clickhouse, :pool, :scale],
      %{active_pools: active_pools, p95_wait_us: p95_wait_us},
      %{backend_id: backend_id}
    )
  end

  @spec reset_samples(__MODULE__.t()) :: __MODULE__.t()
  defp reset_samples(%__MODULE__{} = state) do
    %{state | wait_samples: [], timeout_count: 0}
  end

  @spec schedule_next(__MODULE__.t(), keyword()) :: __MODULE__.t()
  defp schedule_next(%__MODULE__{} = state, config) do
    interval = config[:pool_scale_interval_ms] || 10_000
    timer_ref = Process.send_after(self(), :scale, interval)
    %{state | scale_timer_ref: timer_ref}
  end

  @spec add_sample([non_neg_integer()], non_neg_integer()) :: [non_neg_integer()]
  defp add_sample(samples, value) do
    [value | Enum.take(samples, @sample_window - 1)]
  end

  @spec p95([non_neg_integer()]) :: non_neg_integer()
  defp p95([]), do: 0

  defp p95(samples) do
    sorted = Enum.sort(samples)
    idx = min(trunc(length(sorted) * 0.95), length(sorted) - 1)
    Enum.at(sorted, idx)
  end

  @spec pt_key(pos_integer()) :: {atom(), pos_integer()}
  defp pt_key(backend_id), do: {@pt_prefix, backend_id}

  @spec ch_config() :: keyword()
  defp ch_config do
    Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)
  end
end
