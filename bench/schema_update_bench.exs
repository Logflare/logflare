# Benchmark the local Schema update work without starting the application or
# touching BigQuery/Postgres. This approximates the work inside the Schema
# GenServer before a remote patch would be attempted.
#
# Run with:
#   mix run --no-start bench/schema_update_bench.exs
#
# Optional env vars:
#   SCHEMA_UPDATE_BENCH_SCENARIO=all|wide_top_level|cloudflare_nested|otel_trace
#   BENCH_TIME=5
#   BENCH_WARMUP=2
#   BENCH_MEMORY_TIME=0
#   BENCH_REDUCTION_TIME=0
#   PROFILE_AFTER=false|eprof|tprof|cprof|fprof

Code.require_file("support/schema_helpers_bench.ex", __DIR__)

defmodule Logflare.Profiling.SchemaUpdateBench do
  @moduledoc false

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Profiling.SchemaHelpersBench
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  @type scenario :: SchemaHelpersBench.scenario()
  @type update_result :: :same | {:changed, map(), non_neg_integer()}

  @spec run() :: :ok
  def run do
    scenarios = selected_scenarios()

    Benchee.run(
      %{
        "existing payload: build + diff" => fn scenario ->
          existing_payload_update(scenario)
        end,
        "one new field: build + diff" => fn scenario ->
          new_field_update(scenario)
        end,
        "one new field: build + diff + flatmap" => fn scenario ->
          new_field_update_with_flatmap(scenario)
        end
      },
      benchmark_opts(inputs: benchee_inputs(scenarios))
    )

    :ok
  end

  @spec existing_payload_update(scenario()) :: update_result()
  def existing_payload_update(scenario) do
    scenario
    |> SchemaHelpersBench.payload_for()
    |> local_update(SchemaHelpersBench.schema_for(scenario), false)
  end

  @spec new_field_update(scenario()) :: update_result()
  def new_field_update(scenario) do
    scenario
    |> SchemaHelpersBench.extended_payload_for()
    |> local_update(SchemaHelpersBench.schema_for(scenario), false)
  end

  @spec new_field_update_with_flatmap(scenario()) :: update_result()
  def new_field_update_with_flatmap(scenario) do
    scenario
    |> SchemaHelpersBench.extended_payload_for()
    |> local_update(SchemaHelpersBench.schema_for(scenario), true)
  end

  defp local_update(payload, old_schema, build_flatmap?) do
    new_schema = SchemaBuilder.build_table_schema(payload, old_schema)

    if same_schemas?(old_schema, new_schema) do
      :same
    else
      flatmap =
        if build_flatmap? do
          SchemaUtils.bq_schema_to_flat_typemap(new_schema)
        else
          %{}
        end

      {:changed, new_schema, map_size(flatmap)}
    end
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end

  defp benchmark_opts(overrides) do
    [
      time: benchmark_seconds("BENCH_TIME", 5.0),
      warmup: benchmark_seconds("BENCH_WARMUP", 2.0),
      memory_time: benchmark_seconds("BENCH_MEMORY_TIME", 0.0),
      reduction_time: benchmark_seconds("BENCH_REDUCTION_TIME", 0.0),
      profile_after: profile_after(),
      print: [configuration: false],
      formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
    ]
    |> Keyword.merge(overrides)
  end

  defp profile_after do
    case System.get_env("PROFILE_AFTER", "false") do
      value when value in ["", "false", "0"] -> false
      value when value in ["true", "1"] -> true
      "eprof" -> :eprof
      "tprof" -> :tprof
      "cprof" -> :cprof
      "fprof" -> :fprof
      other -> raise ArgumentError, "Unknown PROFILE_AFTER=#{inspect(other)}"
    end
  end

  defp benchmark_seconds(env_var, default) do
    case System.get_env(env_var) do
      nil ->
        default

      value ->
        case Float.parse(value) do
          {seconds, ""} when seconds >= 0 -> seconds
          _ -> raise ArgumentError, "Expected #{env_var} to be a non-negative number"
        end
    end
  end

  defp selected_scenarios do
    case System.get_env("SCHEMA_UPDATE_BENCH_SCENARIO", "all") do
      "all" -> SchemaHelpersBench.scenario_names()
      "wide_top_level" -> [:wide_top_level]
      "cloudflare_nested" -> [:cloudflare_nested]
      "otel_trace" -> [:otel_trace]
      other -> raise ArgumentError, "Unknown SCHEMA_UPDATE_BENCH_SCENARIO=#{inspect(other)}"
    end
  end

  defp benchee_inputs(scenarios) do
    for scenario <- scenarios, into: %{} do
      {scenario_label(scenario), scenario}
    end
  end

  defp scenario_label(:wide_top_level), do: "wide top-level log"
  defp scenario_label(:cloudflare_nested), do: "cloudflare nested log"
  defp scenario_label(:otel_trace), do: "otel trace"
end

Logflare.Profiling.SchemaUpdateBench.run()
