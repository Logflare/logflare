defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.AsyncInsertFallback do
  @moduledoc """
  Per-node, per-backend fallback from all-batch async inserts on the primary cluster.

  Only subsequent batches change route. The existing retry circuit breaker remains
  responsible for retry shedding; this breaker never retries an insert itself.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Backend

  @window_ms :timer.seconds(30)
  @cooldown_ms :timer.seconds(60)
  @probe_timeout_ms :timer.seconds(45)
  @min_attempts 10
  @min_failures 5
  @failure_ratio 0.5

  defstruct backend_id: nil,
            revision: nil,
            phase: :closed,
            epoch: nil,
            outcomes: [],
            open_until: nil,
            probe_ref: nil,
            probe_started_at: nil

  @type token :: {:normal, reference()} | {:probe, reference()} | nil
  @type route :: {:all, token()} | :split

  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{id: {__MODULE__, backend.id}, start: {__MODULE__, :start_link, [backend]}}
  end

  @spec route(Backend.t(), non_neg_integer()) :: route()
  def route(%Backend{} = backend, row_count) do
    call(backend, {:route, revision(backend), large_batch?(backend, row_count)}, :split)
  end

  @spec record_result(Backend.t(), token(), :ok | {:error, term()}) :: :ok
  def record_result(_backend, nil, _result), do: :ok

  def record_result(%Backend{} = backend, token, result) do
    call(backend, {:record_result, token, classify(result)}, :ok)
  end

  @doc false
  @spec get_state(Backend.t()) :: map() | nil
  def get_state(%Backend{} = backend), do: call(backend, :get_state, nil)

  @impl true
  def init(%Backend{id: backend_id}) do
    {:ok, %__MODULE__{backend_id: backend_id, epoch: make_ref()}}
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  def handle_call({:route, revision, large?}, _from, state) do
    now = System.monotonic_time(:millisecond)
    state = refresh_revision(state, revision)
    {route, state} = choose_route(state, large?, now)
    {:reply, route, state}
  end

  def handle_call(
        {:record_result, {:normal, epoch}, :ignored},
        _from,
        %{phase: :closed, epoch: epoch} = state
      ),
      do: {:reply, :ok, state}

  def handle_call(
        {:record_result, {:normal, epoch}, outcome},
        _from,
        %{phase: :closed, epoch: epoch} = state
      ) do
    now = System.monotonic_time(:millisecond)

    outcomes = [
      {now, outcome} | Enum.filter(state.outcomes, fn {time, _} -> time > now - window_ms() end)
    ]

    state = %{state | outcomes: outcomes}

    state = if trip?(outcomes), do: open(state, now, :failure_threshold), else: state
    {:reply, :ok, state}
  end

  def handle_call(
        {:record_result, {:probe, ref}, outcome},
        _from,
        %{phase: :half_open, probe_ref: ref} = state
      ) do
    now = System.monotonic_time(:millisecond)

    state =
      cond do
        now - state.probe_started_at >= probe_timeout_ms() -> open(state, now, :probe_timeout)
        outcome == :ok -> transition(state, :closed, :probe_succeeded)
        true -> open(state, now, :probe_failed)
      end

    {:reply, :ok, state}
  end

  def handle_call({:record_result, _token, _outcome}, _from, state), do: {:reply, :ok, state}

  @spec call(Backend.t(), term(), term()) :: term()
  defp call(backend, message, fallback) do
    GenServer.call(Backends.via_backend(backend, __MODULE__), message)
  catch
    :exit, _ -> fallback
  end

  @spec revision(Backend.t()) :: tuple()
  defp revision(%Backend{updated_at: updated_at, config: config}) do
    {updated_at, Map.get(config, :async_insert_mode), Map.get(config, :async_insert_max_rows),
     Map.get(config, :async_insert_cluster_url)}
  end

  @spec large_batch?(Backend.t(), non_neg_integer()) :: boolean()
  defp large_batch?(%Backend{config: config}, row_count) do
    case Map.get(config, :async_insert_max_rows) do
      max_rows when is_integer(max_rows) and max_rows > 0 -> row_count >= max_rows
      _ -> true
    end
  end

  @spec refresh_revision(map(), tuple()) :: map()
  defp refresh_revision(%{revision: revision} = state, revision), do: state

  defp refresh_revision(state, revision) do
    state
    |> transition(:closed, :config_changed)
    |> Map.merge(%{
      revision: revision,
      epoch: make_ref(),
      outcomes: [],
      open_until: nil,
      probe_ref: nil,
      probe_started_at: nil
    })
  end

  @spec choose_route(map(), boolean(), integer()) :: {route(), map()}
  defp choose_route(%{phase: :closed} = state, _large?, _now),
    do: {{:all, {:normal, state.epoch}}, state}

  defp choose_route(%{phase: :open, open_until: until} = state, large?, now)
       when now >= until and large? do
    ref = make_ref()
    state = transition(state, :half_open, :cooldown_elapsed)
    {{:all, {:probe, ref}}, %{state | probe_ref: ref, probe_started_at: now}}
  end

  defp choose_route(%{phase: :half_open, probe_started_at: started} = state, _large?, now) do
    if now - started >= probe_timeout_ms() do
      {:split, open(state, now, :probe_timeout)}
    else
      {:split, state}
    end
  end

  defp choose_route(state, _large?, _now), do: {:split, state}

  @spec classify(:ok | {:error, term()}) :: :ok | :failure | :ignored
  defp classify(:ok), do: :ok

  defp classify({:error, reason}) do
    case Ingester.error_class(reason) do
      :timeout -> :failure
      :http_server_error -> if async_server_error?(reason), do: :failure, else: :ignored
      _ -> :ignored
    end
  end

  @spec async_server_error?(term()) :: boolean()
  defp async_server_error?({:http, _status, body}) when is_binary(body) do
    body = String.downcase(body)
    String.contains?(body, "async insert") or String.contains?(body, "async_insert")
  end

  defp async_server_error?(_reason), do: false

  @spec trip?([{integer(), atom()}]) :: boolean()
  defp trip?(outcomes) do
    relevant = Enum.reject(outcomes, fn {_, outcome} -> outcome == :ignored end)
    failures = Enum.count(relevant, fn {_, outcome} -> outcome == :failure end)

    length(relevant) >= setting(:min_attempts, @min_attempts) and
      failures >= setting(:min_failures, @min_failures) and
      failures / length(relevant) >= setting(:failure_ratio, @failure_ratio)
  end

  @spec open(map(), integer(), atom()) :: map()
  defp open(state, now, reason) do
    state
    |> transition(:open, reason)
    |> Map.merge(%{
      outcomes: [],
      open_until: now + setting(:cooldown_ms, @cooldown_ms),
      probe_ref: nil,
      probe_started_at: nil
    })
  end

  @spec transition(map(), atom(), atom()) :: map()
  defp transition(%{phase: phase} = state, phase, _reason), do: state

  defp transition(state, phase, reason) do
    Logger.warning("ClickHouse async insert fallback changed state",
      backend_id: state.backend_id,
      from: state.phase,
      to: phase,
      reason: reason
    )

    :telemetry.execute(
      [:logflare, :clickhouse, :async_insert_fallback, :transition],
      %{count: 1},
      %{backend_id: state.backend_id, from: state.phase, to: phase, reason: reason}
    )

    %{state | phase: phase, epoch: make_ref(), probe_ref: nil, probe_started_at: nil}
  end

  @spec window_ms() :: pos_integer()
  defp window_ms, do: setting(:window_ms, @window_ms)

  @spec probe_timeout_ms() :: pos_integer()
  defp probe_timeout_ms, do: setting(:probe_timeout_ms, @probe_timeout_ms)

  @spec setting(atom(), term()) :: term()
  defp setting(key, default),
    do: Application.get_env(:logflare, __MODULE__, []) |> Keyword.get(key, default)
end
