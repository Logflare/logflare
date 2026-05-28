defmodule Logflare.UserMetrics.MetricStore do
  @moduledoc false

  use GenServer

  require Logger

  alias Telemetry.Metrics

  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.{
    Metric,
    NumberDataPoint,
    HistogramDataPoint,
    Sum,
    Gauge,
    Histogram
  }

  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{
    AnyValue,
    KeyValue
  }

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
  @default_max_table_memory 2_000_000_000

  @current_gen_idx 1
  @earliest_gen_idx 2
  @max_counter_value 2 ** 64 - 1

  defmodule State do
    @moduledoc false
    defstruct [:config, :metrics, :metrics_table, :generations_table, :partial_gen]

    @type t :: %__MODULE__{
            config: map(),
            metrics: list(),
            metrics_table: atom(),
            generations_table: :ets.tid(),
            partial_gen: non_neg_integer() | nil
          }
  end

  @doc false
  def default_buckets, do: @default_buckets

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, genserver_opts(config))
  end

  defp genserver_opts(config) do
    [
      name: config.name,
      hibernate_after: config[:hibernate_after],
      spawn_opt: config[:spawn_opt]
    ]
    |> Enum.filter(fn {_, v} -> v end)
  end

  def get_metrics(metrics_table, generation \\ nil) do
    generation = generation || get_current_gen(metrics_table)

    :ets.match_object(metrics_table, {{generation, :_, :_, :_, :_}, :_, :_})
    |> group_rows()
  end

  defp group_rows(rows) do
    Enum.reduce(rows, %{}, fn
      {{_, name, :distribution, tags, bucket}, count, sum}, acc ->
        Map.update(
          acc,
          {:distribution, name},
          %{tags => %{bucket => {count, sum}}},
          fn all_tags ->
            Map.update(all_tags, tags, %{bucket => {count, sum}}, fn all_buckets ->
              Map.put(all_buckets, bucket, {count, sum})
            end)
          end
        )

      {{_, name, type, tags, _}, value, _}, acc ->
        Map.update(acc, {type, name}, %{tags => value}, fn all_tags ->
          Map.put(all_tags, tags, value)
        end)
    end)
  end

  def pull(name, limit \\ :infinity)

  def pull(name, :infinity) do
    with {:ok, collector} <- GenServer.call(name, :prepare_to_collect) do
      collector.()
    end
  end

  def pull(name, limit) when is_integer(limit) and limit > 0 do
    GenServer.call(name, {:prepare_to_collect, limit})
  end

  @spec record_count(atom()) :: non_neg_integer()
  def record_count(name), do: :ets.info(name, :size)

  defp metric_type(%Metrics.Counter{}), do: :counter
  defp metric_type(%Metrics.Sum{}), do: :sum
  defp metric_type(%Metrics.LastValue{}), do: :last_value
  defp metric_type(%Metrics.Distribution{}), do: :distribution

  def write_metric(metrics_table, metric, value, tags),
    do: write_metric(metrics_table, metric, Enum.join(metric.name, "."), value, tags)

  def write_metric(metrics_table, %Metrics.Counter{} = metric, string_name, _, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}
    :ets.update_counter(metrics_table, ets_key, 1, {ets_key, 0, nil})
  end

  def write_metric(_metrics_table, %Metrics.Sum{} = _metric, _string_name, value, _tags)
      when not is_number(value),
      do: :ok

  def write_metric(metrics_table, %Metrics.Sum{} = metric, string_name, value, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}
    :ets.update_counter(metrics_table, ets_key, value, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.LastValue{} = metric, string_name, value, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}
    :ets.update_element(metrics_table, ets_key, {2, value}, {ets_key, value, nil})
  end

  def write_metric(
        _metrics_table,
        %Metrics.Distribution{} = _metric,
        _string_name,
        value,
        _tags
      )
      when not is_number(value),
      do: :ok

  def write_metric(
        metrics_table,
        %Metrics.Distribution{} = metric,
        string_name,
        value,
        tags
      ) do
    bucket = find_bucket(metric, value)
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, bucket}

    :ets.update_counter(
      metrics_table,
      ets_key,
      [{2, 1}, {3, round(value)}],
      {ets_key, 0, 0}
    )
  end

  defp find_bucket(%Metrics.Distribution{reporter_options: opts}, value) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)

    case Enum.find_index(bucket_bounds, &(value <= &1)) do
      nil -> length(bucket_bounds)
      idx -> idx
    end
  end

  @impl true
  def init(config) do
    metrics = Map.get(config, :metrics, [])
    metrics_table = config.name

    config = Map.put_new(config, :max_table_memory, @default_max_table_memory)

    Process.send_after(self(), :rotate_and_trim, config.export_period)

    :ets.new(metrics_table, [:ordered_set, :public, :named_table, {:write_concurrency, :auto}])

    generations_table = :ets.new(:generations, [:ordered_set, :private])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    generation_counters = :atomics.new(2, signed: false)
    :persistent_term.put(generation_key(metrics_table), generation_counters)

    {:ok,
     %State{
       config: config,
       metrics: metrics,
       metrics_table: metrics_table,
       generations_table: generations_table
     }}
  end

  @impl true
  def handle_call(:prepare_to_collect, _from, state) do
    current_gen = get_current_gen(state.metrics_table)
    earliest_gen = bump_earliest_gen(state.metrics_table)

    if current_gen == earliest_gen do
      rotate_generation(state)
    end

    gen_meta = pop_generation(state, earliest_gen)

    collector = fn ->
      metrics = collect_metrics(state, gen_meta) |> transform_metrics(state)
      :ets.match_delete(state.metrics_table, {{earliest_gen, :_, :_, :_, :_}, :_, :_})
      {:ok, metrics}
    end

    {:reply, {:ok, collector}, state}
  end

  @impl true
  def handle_call({:prepare_to_collect, limit}, _from, state) do
    {gen, state} =
      if state.partial_gen != nil do
        {state.partial_gen, state}
      else
        earliest_gen = bump_earliest_gen(state.metrics_table)
        current_gen = get_current_gen(state.metrics_table)
        if current_gen == earliest_gen, do: rotate_generation(state)
        {earliest_gen, state}
      end

    gen_meta = lookup_generation(state, gen)
    pattern = {{gen, :_, :_, :_, :_}, :_, :_}

    metrics =
      case :ets.match_object(state.metrics_table, pattern, limit) do
        :"$end_of_table" ->
          []

        {objects, _continuation} ->
          for obj <- objects, do: :ets.delete_object(state.metrics_table, obj)
          collect_metrics_bounded(objects, gen_meta) |> transform_metrics(state)
      end

    state =
      case :ets.match_object(state.metrics_table, pattern, 1) do
        :"$end_of_table" ->
          :ets.delete(state.generations_table, gen)
          %{state | partial_gen: nil}

        _ ->
          %{state | partial_gen: gen}
      end

    {:reply, {:ok, metrics}, state}
  end

  @impl true
  def handle_info(:rotate_and_trim, state) do
    rotate_generation(state)
    Process.send_after(self(), :rotate_and_trim, state.config.export_period)
    {:noreply, state}
  end

  defp rotate_generation(%State{} = state) do
    {old_gen, new_gen} = bump_gen_counter(state.metrics_table)

    :ets.update_element(
      state.generations_table,
      old_gen,
      {3, System.system_time(:nanosecond)}
    )

    :ets.insert(state.generations_table, {new_gen, System.system_time(:nanosecond), nil})

    trim_metrics_table(state)

    old_gen
  end

  defp trim_metrics_table(state) do
    if above_memory_limit?(state) do
      earliest_gen = earliest_gen(state.generations_table)
      current_gen = get_current_gen(state.metrics_table)
      previous_gen = current_gen - 1

      earliest_gen..previous_gen
      |> Enum.take_while(fn gen ->
        clear_generations(state, gen..gen)
        above_memory_limit?(state)
      end)
    end
  end

  defp above_memory_limit?(%{config: %{max_table_memory: nil}}), do: false

  defp above_memory_limit?(state) do
    memory_size = :ets.info(state.metrics_table, :memory) * :erlang.system_info(:wordsize)
    memory_size > state.config[:max_table_memory]
  end

  defp collect_metrics(state, {gen, start, finish}) do
    get_metrics(state.metrics_table, gen)
    |> Enum.map(fn {key, values} ->
      tagged_values = Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)
      {key, tagged_values}
    end)
  end

  defp collect_metrics_bounded(rows, {_, start, finish}) do
    group_rows(rows)
    |> Enum.map(fn {key, values} ->
      tagged_values = Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)
      {key, tagged_values}
    end)
  end

  defp transform_metrics(raw_metrics, state) do
    raw_metrics
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {{type, name}, grouped_values} ->
      metric =
        Enum.find(state.metrics, &(Enum.join(&1.name, ".") == name and metric_type(&1) == type))

      convert_metric(metric, List.flatten(grouped_values))
    end)
  end

  defp convert_metric(
         %{name: name, description: description, unit: unit} = metric,
         values
       ) do
    %Metric{
      name: Enum.join(name, "."),
      description: description,
      unit: convert_unit(unit),
      data: convert_data(metric, values)
    }
  end

  defp convert_data(%Metrics.Counter{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_int, value}
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: true
     }}
  end

  defp convert_data(%Metrics.Sum{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_int, value}
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: false
     }}
  end

  defp convert_data(%Metrics.LastValue{}, values) do
    {:gauge,
     %Gauge{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_double, value}
           }
         end)
     }}
  end

  defp convert_data(%Metrics.Distribution{reporter_options: opts}, values) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)
    total_bucket_bounds = length(bucket_bounds)

    {:histogram,
     %Histogram{
       data_points:
         Enum.map(values, fn {{from, to}, tags, bucket_values} ->
           {total_count, total_sum} =
             Enum.reduce(bucket_values, {0, 0.0}, fn {_, {count, sum}},
                                                     {total_count, total_sum} ->
               {total_count + count, total_sum + sum}
             end)

           bucket_counts =
             Enum.map(0..total_bucket_bounds//1, &elem(Map.get(bucket_values, &1, {0, 0}), 0))

           %HistogramDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             count: total_count,
             sum: total_sum,
             bucket_counts: bucket_counts,
             explicit_bounds: bucket_bounds
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
     }}
  end

  defp convert_unit(:unit), do: nil
  defp convert_unit(:second), do: "s"
  defp convert_unit(:millisecond), do: "ms"
  defp convert_unit(:microsecond), do: "us"
  defp convert_unit(:nanosecond), do: "ns"
  defp convert_unit(:byte), do: "By"
  defp convert_unit(:kilobyte), do: "kBy"
  defp convert_unit(:megabyte), do: "MBy"
  defp convert_unit(:gigabyte), do: "GBy"
  defp convert_unit(:terabyte), do: "TBy"
  defp convert_unit(x) when is_atom(x), do: Atom.to_string(x)

  defp build_kv(tags, key_prefix \\ "") do
    Enum.flat_map(tags, fn {key, value} ->
      if is_map(value) do
        build_kv(value, key_prefix <> to_string(key) <> ".")
      else
        [
          %KeyValue{
            key: key_prefix <> to_string(key),
            value: %AnyValue{value: to_kv_value(value)}
          }
        ]
      end
    end)
  end

  defp to_kv_value(value) when is_binary(value), do: {:string_value, value}
  defp to_kv_value(value) when is_atom(value), do: {:string_value, to_string(value)}
  defp to_kv_value(value) when is_integer(value), do: {:int_value, value}
  defp to_kv_value(value) when is_float(value), do: {:double_value, value}
  defp to_kv_value(value) when is_boolean(value), do: {:bool_value, value}
  defp to_kv_value(value) when is_struct(value), do: {:string_value, to_string(value)}
  defp to_kv_value(value) when is_pid(value), do: to_kv_value(inspect(value))
  defp to_kv_value(value) when is_tuple(value), do: to_kv_value(Tuple.to_list(value))

  defp to_kv_value(value) when is_list(value),
    do:
      {:array_value,
       %OtelMetricExporter.Opentelemetry.Proto.Common.V1.ArrayValue{
         values: Enum.map(value, &%AnyValue{value: to_kv_value(&1)})
       }}

  defp to_kv_value(any), do: to_kv_value(inspect(any))

  defp lookup_generation(state, gen) do
    :ets.lookup(state.generations_table, gen)
    |> List.first({nil, nil, nil})
  end

  defp pop_generation(state, gen) do
    :ets.take(state.generations_table, gen)
    |> List.first({nil, nil, nil})
  end

  defp clear_generations(state, range) do
    for gen <- range do
      :ets.match_delete(state.metrics_table, {{gen, :_, :_, :_, :_}, :_, :_})
      :ets.delete(state.generations_table, gen)
    end
  end

  defp earliest_gen(generations_table) do
    case :ets.first(generations_table) do
      :"$end_of_table" -> 0
      x -> x
    end
  end

  defp get_current_gen(table) do
    counters = :persistent_term.get(generation_key(table))
    :atomics.get(counters, @current_gen_idx)
  end

  defp bump_gen_counter(table) do
    counters = :persistent_term.get(generation_key(table))
    next_gen = :atomics.add_get(counters, @current_gen_idx, 1)
    old_gen = if next_gen != 0, do: next_gen - 1, else: @max_counter_value
    {old_gen, next_gen}
  end

  defp bump_earliest_gen(table) do
    counters = :persistent_term.get(generation_key(table))
    next_gen = :atomics.add_get(counters, @earliest_gen_idx, 1)
    if next_gen != 0, do: next_gen - 1, else: @max_counter_value
  end

  defp generation_key(metrics_table) do
    {__MODULE__, metrics_table, :generation}
  end
end
