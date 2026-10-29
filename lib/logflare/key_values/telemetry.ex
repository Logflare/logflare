defmodule Logflare.KeyValues.Telemetry do
  @moduledoc false

  import Logflare.Utils.Guards, only: [is_pos_integer: 1]

  @state_key {__MODULE__, :state}
  @operations [:lookup, :extraction, :database_fetch]
  @accessor_kinds [:whole, :dot, :jsonpath]
  @results [:value, :nil_value, :error]
  @sources [:warmed, :database]
  @default_denominators [lookup: 1024, extraction: 128, database_fetch: 16]
  @observations (for operation <- @operations, kind <- @accessor_kinds, result <- @results do
                   {operation, kind, result}
                 end)
  @fills (for kind <- @accessor_kinds, source <- @sources do
            {:fill, kind, source}
          end)
  @series @observations ++ @fills
  @indexes @series |> Enum.with_index(1) |> Map.new()

  @type operation :: :lookup | :extraction | :database_fetch
  @type accessor :: String.t() | nil

  @spec initialize() :: :ok
  def initialize do
    case :persistent_term.get(@state_key, nil) do
      nil ->
        denominators = sample_denominators!()
        size = length(@series)

        :persistent_term.put(@state_key, %{
          counters: :counters.new(size, [:write_concurrency]),
          previous: :atomics.new(size, signed: false),
          denominators: denominators
        })

      _state ->
        :ok
    end
  end

  @spec observe(operation(), accessor(), (-> result)) :: result when result: term()
  def observe(operation, accessor, fun) when operation in @operations do
    case :persistent_term.get(@state_key, nil) do
      nil -> fun.()
      state -> observe(state, operation, accessor_kind(accessor), fun)
    end
  end

  @spec fill(accessor(), :warmed | :database) :: :ok
  def fill(accessor, source) when source in @sources do
    case :persistent_term.get(@state_key, nil) do
      nil ->
        :ok

      state ->
        index = 27 + accessor_ordinal(accessor_kind(accessor)) * 2 + source_ordinal(source)
        increment(state, index)
    end
  end

  @doc false
  @spec sample?(operation()) :: boolean()
  def sample?(operation) when operation in @operations do
    case :persistent_term.get(@state_key, nil) do
      nil -> false
      %{denominators: denominators} -> sample_denominator?(Map.fetch!(denominators, operation))
    end
  end

  @spec poll() :: :ok
  def poll do
    case :persistent_term.get(@state_key, nil) do
      nil -> :ok
      state -> Enum.each(@series, &emit_count(state, &1))
    end
  end

  @spec observe(map(), operation(), atom(), (-> result)) :: result when result: term()
  defp observe(state, operation, kind, fun) do
    denominator = Map.fetch!(state.denominators, operation)
    started_at = if sample_denominator?(denominator), do: System.monotonic_time()

    try do
      fun.()
    catch
      failure, reason ->
        stacktrace = __STACKTRACE__
        finish(state, operation, kind, :error, started_at)
        :erlang.raise(failure, reason, stacktrace)
    else
      value ->
        finish(state, operation, kind, result(value), started_at)
        value
    end
  end

  @spec finish(map(), operation(), atom(), atom(), integer() | nil) :: :ok
  defp finish(state, operation, kind, result, started_at) do
    duration = if started_at, do: System.monotonic_time() - started_at
    index = operation_offset(operation) + accessor_ordinal(kind) * 3 + result_ordinal(result)
    increment(state, index)
    emit_duration(state, operation, kind, result, duration)
  end

  @spec emit_duration(map(), operation(), atom(), atom(), integer() | nil) :: :ok
  defp emit_duration(_state, _operation, _kind, _result, nil), do: :ok

  defp emit_duration(state, operation, kind, result, duration) do
    :telemetry.execute(
      [:logflare, :key_values, operation, :stop],
      %{duration: duration},
      %{
        implementation: :accessor_cache,
        accessor_kind: kind,
        result: result,
        sample_denominator: Map.fetch!(state.denominators, operation)
      }
    )
  end

  @spec emit_count(map(), tuple()) :: :ok
  defp emit_count(state, series) do
    index = Map.fetch!(@indexes, series)
    count = claim_delta(state, index, 3)
    emit_delta(series, count)
  end

  @spec emit_delta(tuple(), non_neg_integer()) :: :ok
  defp emit_delta(_series, 0), do: :ok

  defp emit_delta({:fill, kind, source}, count) do
    :telemetry.execute(
      [:logflare, :key_values, :fill],
      %{count: count},
      %{implementation: :accessor_cache, accessor_kind: kind, source: source}
    )
  end

  defp emit_delta({operation, kind, result}, count) do
    :telemetry.execute(
      [:logflare, :key_values, operation],
      %{count: count},
      %{implementation: :accessor_cache, accessor_kind: kind, result: result}
    )
  end

  @spec claim_delta(map(), pos_integer(), non_neg_integer()) :: non_neg_integer()
  defp claim_delta(_state, _index, 0), do: 0

  defp claim_delta(state, index, attempts) do
    total = :counters.get(state.counters, index)
    previous = :atomics.get(state.previous, index)

    if total > previous do
      case :atomics.compare_exchange(state.previous, index, previous, total) do
        :ok -> total - previous
        _changed -> claim_delta(state, index, attempts - 1)
      end
    else
      0
    end
  end

  @spec increment(map(), pos_integer()) :: :ok
  defp increment(state, index), do: :counters.add(state.counters, index, 1)

  @spec operation_offset(operation()) :: 0 | 9 | 18
  defp operation_offset(:lookup), do: 0
  defp operation_offset(:extraction), do: 9
  defp operation_offset(:database_fetch), do: 18

  @spec accessor_ordinal(:whole | :dot | :jsonpath) :: 0 | 1 | 2
  defp accessor_ordinal(:whole), do: 0
  defp accessor_ordinal(:dot), do: 1
  defp accessor_ordinal(:jsonpath), do: 2

  @spec result_ordinal(:value | :nil_value | :error) :: 1 | 2 | 3
  defp result_ordinal(:value), do: 1
  defp result_ordinal(:nil_value), do: 2
  defp result_ordinal(:error), do: 3

  @spec source_ordinal(:warmed | :database) :: 1 | 2
  defp source_ordinal(:warmed), do: 1
  defp source_ordinal(:database), do: 2

  @spec accessor_kind(accessor()) :: :whole | :dot | :jsonpath
  defp accessor_kind(nil), do: :whole
  defp accessor_kind("$" <> _rest), do: :jsonpath
  defp accessor_kind(_accessor), do: :dot

  @spec result(term()) :: :value | :nil_value
  defp result(nil), do: :nil_value
  defp result(_value), do: :value

  @spec sample_denominator?(pos_integer() | :disabled) :: boolean()
  defp sample_denominator?(:disabled), do: false
  defp sample_denominator?(1), do: true
  defp sample_denominator?(denominator), do: :rand.uniform(denominator) == 1

  @spec sample_denominators!() :: map()
  defp sample_denominators! do
    denominators =
      :logflare
      |> Application.get_env(__MODULE__, [])
      |> Keyword.get(:sample_denominators, [])
      |> Keyword.validate!(@default_denominators)

    Enum.each(denominators, fn
      {_operation, :disabled} ->
        :ok

      {_operation, denominator} when is_pos_integer(denominator) ->
        :ok

      {operation, value} ->
        raise ArgumentError, "invalid #{operation} sample denominator: #{inspect(value)}"
    end)

    Map.new(denominators)
  end
end
