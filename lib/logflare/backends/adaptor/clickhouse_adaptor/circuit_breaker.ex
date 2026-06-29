defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker do
  @moduledoc """
  Per-backend circuit breaker for ClickHouse insert retries, backed by ETS.

  Trips when the number of insert failures within `window_ms` reaches
  `max_failures`, after which `check/1` reports the breaker open for `block_ms`.
  The pipeline uses this to shed **retries** while open;
  initial insert attempts are never blocked. This stops requeued retries from
  compounding load on a struggling ClickHouse cluster.
  """

  use GenServer
  use TypedStruct

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry

  @blocked_key :blocked_until
  @max_failures 50
  @window_ms :timer.seconds(30)
  @block_ms :timer.seconds(30)

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :table, :ets.table(), enforce: true
    field :failures, [integer()], default: []
  end

  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, backend.id},
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc """
  Returns `:ok` if inserts are allowed, or `{:error, :circuit_open, blocked_until}`
  when the breaker is open.

  Reads the breaker's ETS table directly via the backend registry, so a missing
  or crashed breaker reads as allowed.
  """
  @spec check(Backend.t() | pos_integer()) :: :ok | {:error, :circuit_open, integer()}
  def check(%Backend{id: backend_id}), do: check(backend_id)

  def check(backend_id) when is_pos_integer(backend_id) do
    with [{_pid, table}] when not is_nil(table) <-
           Registry.lookup(BackendRegistry, {__MODULE__, backend_id}),
         [{@blocked_key, blocked_until}] <- safe_lookup(table, @blocked_key),
         true <- blocked_until > System.system_time(:millisecond) do
      {:error, :circuit_open, blocked_until}
    else
      _ -> :ok
    end
  end

  @doc """
  Records a single insert failure for the backend.

  Best-effort async cast. The breaker trips once `max_failures` failures land
  within `window_ms`.
  """
  @spec record_failure(Backend.t()) :: :ok
  def record_failure(%Backend{id: backend_id}) do
    case Registry.lookup(BackendRegistry, {__MODULE__, backend_id}) do
      [{pid, _table}] -> GenServer.cast(pid, :record_failure)
      [] -> :ok
    end
  end

  @doc """
  Returns the breaker's current state, or `nil` if no breaker is running.

  Useful for introspection (_and tests_).
  """
  @spec get_state(Backend.t() | pos_integer()) :: t() | nil
  def get_state(%Backend{id: backend_id}), do: get_state(backend_id)

  def get_state(backend_id) when is_pos_integer(backend_id) do
    case Registry.lookup(BackendRegistry, {__MODULE__, backend_id}) do
      [{pid, _table}] -> GenServer.call(pid, :get_state)
      [] -> nil
    end
  end

  @impl true
  def init(%Backend{id: backend_id}) do
    table = :ets.new(:clickhouse_circuit_breaker, [:set, :public, read_concurrency: true])
    Registry.update_value(BackendRegistry, {__MODULE__, backend_id}, fn _ -> table end)
    {:ok, %__MODULE__{backend_id: backend_id, table: table}}
  end

  @impl true
  def handle_call(:get_state, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast(:record_failure, %__MODULE__{} = state) do
    now = System.system_time(:millisecond)
    cutoff = now - window_ms()
    failures = Enum.filter([now | state.failures], &(&1 >= cutoff))

    {:noreply, maybe_trip(state, failures, now)}
  end

  @spec maybe_trip(t(), [integer()], integer()) :: t()
  defp maybe_trip(%__MODULE__{} = state, failures, now) do
    if length(failures) >= max_failures() do
      blocked_until = now + block_ms()
      :ets.insert(state.table, {@blocked_key, blocked_until})

      Logger.warning("ClickHouse circuit breaker opened",
        backend_id: state.backend_id,
        blocked_until: blocked_until,
        failures: length(failures)
      )

      :telemetry.execute(
        [:logflare, :clickhouse, :circuit_breaker, :open],
        %{failures: length(failures)},
        %{backend_id: state.backend_id}
      )

      %{state | failures: []}
    else
      %{state | failures: failures}
    end
  end

  @spec safe_lookup(:ets.table(), term()) :: [tuple()]
  defp safe_lookup(table, key) do
    :ets.lookup(table, key)
  rescue
    ArgumentError -> []
  end

  @spec max_failures() :: pos_integer()
  defp max_failures,
    do: Application.get_env(:logflare, __MODULE__)[:max_failures] || @max_failures

  @spec window_ms() :: pos_integer()
  defp window_ms, do: Application.get_env(:logflare, __MODULE__)[:window_ms] || @window_ms

  @spec block_ms() :: pos_integer()
  defp block_ms, do: Application.get_env(:logflare, __MODULE__)[:block_ms] || @block_ms
end
