defmodule Logflare.Sources.Source.BigQuery.SchemaMetrics do
  @moduledoc false

  use GenServer

  @counter_key {__MODULE__, :counters}
  @counter_count 6
  @selected 1
  @selected_zero_rate 2
  @selected_floor 3
  @admitted 4
  @rejected 5
  @handled 6

  def start_link(_args) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @spec record_sample(:normal | :zero_rate | :floor) :: :ok
  def record_sample(mode) do
    increment(@selected)

    case mode do
      :zero_rate -> increment(@selected_zero_rate)
      :floor -> increment(@selected_floor)
      :normal -> :ok
    end
  end

  @spec record_admission(:admitted | :rejected) :: :ok
  def record_admission(:admitted), do: increment(@admitted)
  def record_admission(:rejected), do: increment(@rejected)

  @spec record_handled() :: :ok
  def record_handled, do: increment(@handled)

  @spec report() :: :ok
  def report do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.call(__MODULE__, :report)
    end
  end

  @doc false
  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @impl GenServer
  def init(_args) do
    counters = :counters.new(@counter_count, [:write_concurrency])
    :persistent_term.put(@counter_key, counters)
    {:ok, %{counters: counters, previous: zero_counts()}}
  end

  @impl GenServer
  def handle_call(:report, _from, state) do
    current = read_counts(state.counters)
    deltas = Enum.zip_with(current, state.previous, &max(&1 - &2, 0))

    :telemetry.execute(
      [:logflare, :bigquery, :schema, :report],
      counter_measurements(deltas),
      %{}
    )

    {:reply, :ok, %{state | previous: current}}
  end

  def handle_call(:reset, _from, state) do
    for index <- 1..@counter_count, do: :counters.put(state.counters, index, 0)
    {:reply, :ok, %{state | previous: zero_counts()}}
  end

  @impl GenServer
  def terminate(_reason, %{counters: counters}) do
    if :persistent_term.get(@counter_key, nil) == counters do
      :persistent_term.erase(@counter_key)
    end

    :ok
  end

  defp increment(index) do
    case :persistent_term.get(@counter_key, nil) do
      nil -> :ok
      counters -> :counters.add(counters, index, 1)
    end

    :ok
  end

  defp zero_counts, do: List.duplicate(0, @counter_count)

  defp read_counts(counters),
    do: for(index <- 1..@counter_count, do: :counters.get(counters, index))

  defp counter_measurements([
         selected,
         selected_zero_rate,
         selected_floor,
         admitted,
         rejected,
         handled
       ]) do
    %{
      samples_selected: selected,
      samples_selected_zero_rate: selected_zero_rate,
      samples_selected_floor: selected_floor,
      samples_admitted: admitted,
      samples_rejected: rejected,
      samples_handled: handled
    }
  end
end
