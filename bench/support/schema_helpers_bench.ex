defmodule Logflare.Profiling.SchemaHelpersBench do
  @moduledoc false

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias LogflareWeb.Logs.PayloadTestUtils

  @data_key {__MODULE__, :data}

  @type suite :: :all | :builder | :payload_helpers | :schema_helpers
  @type scenario :: :cloudflare_nested | :otel_trace | :wide_top_level

  @spec run() :: :ok
  def run do
    run(selected_suite())
  end

  @spec run(suite()) :: :ok
  def run(:all) do
    Enum.each([:payload_helpers, :builder, :schema_helpers], &run/1)
    :ok
  end

  def run(:payload_helpers) do
    IO.puts("\n== Schema payload helpers ==")

    Benchee.run(
      %{
        "to_typemap/1" => fn scenario ->
          payload_to_typemap(scenario)
        end,
        "flatten_typemap/1 (precomputed payload typemap)" => fn scenario ->
          payload_flatten_typemap(scenario)
        end,
        "to_typemap |> flatten_typemap" => fn scenario ->
          payload_to_flat_typemap(scenario)
        end
      },
      benchmark_opts(inputs: benchee_inputs())
    )

    :ok
  end

  def run(:builder) do
    IO.puts("\n== Schema builder helpers ==")

    Benchee.run(
      %{
        "build_table_schema/2 from initial schema" => fn scenario ->
          builder_from_initial(scenario)
        end,
        "build_table_schema/2 against existing schema" => fn scenario ->
          builder_against_existing(scenario)
        end,
        "build_table_schema/2 with 1 new field" => fn scenario ->
          builder_with_new_field(scenario)
        end
      },
      benchmark_opts(inputs: benchee_inputs())
    )

    :ok
  end

  def run(:schema_helpers) do
    IO.puts("\n== BigQuery schema helpers ==")

    Benchee.run(
      %{
        "to_typemap/1 on TableSchema" => fn scenario ->
          schema_to_typemap(scenario)
        end,
        "flatten_typemap/1 (precomputed schema typemap)" => fn scenario ->
          schema_flatten_typemap(scenario)
        end,
        "bq_schema_to_flat_typemap/1" => fn scenario ->
          schema_to_flat_typemap(scenario)
        end
      },
      benchmark_opts(inputs: benchee_inputs())
    )

    :ok
  end

  @spec payload_to_typemap(scenario()) :: map()
  def payload_to_typemap(scenario) do
    scenario
    |> payload_for()
    |> SchemaUtils.to_typemap()
  end

  @spec payload_flatten_typemap(scenario()) :: map()
  def payload_flatten_typemap(scenario) do
    scenario
    |> payload_typemap_for()
    |> SchemaUtils.flatten_typemap()
  end

  @spec payload_to_flat_typemap(scenario()) :: map()
  def payload_to_flat_typemap(scenario) do
    scenario
    |> payload_for()
    |> SchemaUtils.to_typemap()
    |> SchemaUtils.flatten_typemap()
  end

  @spec builder_from_initial(scenario()) :: GoogleApi.BigQuery.V2.Model.TableSchema.t()
  def builder_from_initial(scenario) do
    SchemaBuilder.build_table_schema(payload_for(scenario), initial_schema())
  end

  @spec builder_against_existing(scenario()) :: GoogleApi.BigQuery.V2.Model.TableSchema.t()
  def builder_against_existing(scenario) do
    SchemaBuilder.build_table_schema(payload_for(scenario), schema_for(scenario))
  end

  @spec builder_with_new_field(scenario()) :: GoogleApi.BigQuery.V2.Model.TableSchema.t()
  def builder_with_new_field(scenario) do
    SchemaBuilder.build_table_schema(extended_payload_for(scenario), schema_for(scenario))
  end

  @spec schema_to_typemap(scenario()) :: map()
  def schema_to_typemap(scenario) do
    scenario
    |> schema_for()
    |> SchemaUtils.to_typemap()
  end

  @spec schema_flatten_typemap(scenario()) :: map()
  def schema_flatten_typemap(scenario) do
    scenario
    |> schema_typemap_for()
    |> SchemaUtils.flatten_typemap()
  end

  @spec schema_to_flat_typemap(scenario()) :: map()
  def schema_to_flat_typemap(scenario) do
    scenario
    |> schema_for()
    |> SchemaUtils.bq_schema_to_flat_typemap()
  end

  @spec repeat(pos_integer(), (-> term())) :: :ok
  def repeat(count, fun) when is_integer(count) and count > 0 and is_function(fun, 0) do
    do_repeat(count, fun)
  end

  @spec initial_schema() :: GoogleApi.BigQuery.V2.Model.TableSchema.t()
  def initial_schema do
    data().initial_schema
  end

  @spec scenario_names() :: [scenario()]
  def scenario_names do
    data().scenario_names
  end

  @spec payload_for(scenario()) :: map()
  def payload_for(scenario) do
    Map.fetch!(data().payloads, scenario)
  end

  @spec extended_payload_for(scenario()) :: map()
  def extended_payload_for(scenario) do
    Map.fetch!(data().extended_payloads, scenario)
  end

  @spec schema_for(scenario()) :: GoogleApi.BigQuery.V2.Model.TableSchema.t()
  def schema_for(scenario) do
    Map.fetch!(data().schemas, scenario)
  end

  defp do_repeat(0, _fun), do: :ok

  defp do_repeat(count, fun) do
    fun.()
    do_repeat(count - 1, fun)
  end

  defp payload_typemap_for(scenario) do
    Map.fetch!(data().payload_typemaps, scenario)
  end

  defp schema_typemap_for(scenario) do
    Map.fetch!(data().schema_typemaps, scenario)
  end

  defp selected_suite do
    case System.get_env("SCHEMA_BENCH_SUITE", "all") do
      "all" -> :all
      "payload" -> :payload_helpers
      "payload_helpers" -> :payload_helpers
      "builder" -> :builder
      "schema" -> :schema_helpers
      "schema_helpers" -> :schema_helpers
      other -> raise ArgumentError, "Unknown SCHEMA_BENCH_SUITE=#{inspect(other)}"
    end
  end

  defp benchmark_opts(overrides) do
    [
      time: benchmark_seconds("BENCH_TIME", 5.0),
      warmup: benchmark_seconds("BENCH_WARMUP", 2.0),
      memory_time: benchmark_seconds("BENCH_MEMORY_TIME", 3.0),
      reduction_time: benchmark_seconds("BENCH_REDUCTION_TIME", 3.0),
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

  defp benchee_inputs do
    for scenario <- scenario_names(), into: %{} do
      {scenario_label(scenario), scenario}
    end
  end

  defp scenario_label(:wide_top_level), do: "wide top-level log"
  defp scenario_label(:cloudflare_nested), do: "cloudflare nested log"
  defp scenario_label(:otel_trace), do: "otel trace"

  defp data do
    case :persistent_term.get(@data_key, nil) do
      nil ->
        data = build_data()
        :persistent_term.put(@data_key, data)
        data

      data ->
        data
    end
  end

  defp build_data do
    initial_schema = SchemaBuilder.initial_table_schema()

    payloads = %{
      wide_top_level: wide_top_level_payload(),
      cloudflare_nested: cloudflare_nested_payload(),
      otel_trace: otel_trace_payload()
    }

    schemas =
      for {scenario, payload} <- payloads, into: %{} do
        {scenario, SchemaBuilder.build_table_schema(payload, initial_schema)}
      end

    payload_typemaps =
      for {scenario, payload} <- payloads, into: %{} do
        {scenario, SchemaUtils.to_typemap(payload)}
      end

    schema_typemaps =
      for {scenario, schema} <- schemas, into: %{} do
        {scenario, SchemaUtils.to_typemap(schema)}
      end

    %{
      scenario_names: [:wide_top_level, :cloudflare_nested, :otel_trace],
      initial_schema: initial_schema,
      payloads: payloads,
      extended_payloads: extended_payloads(payloads),
      payload_typemaps: payload_typemaps,
      schemas: schemas,
      schema_typemaps: schema_typemaps
    }
  end

  defp extended_payloads(payloads) do
    %{
      wide_top_level:
        put_nested(
          Map.fetch!(payloads, :wide_top_level),
          ["nested_metrics", "queue", "lag_ms"],
          33
        ),
      cloudflare_nested:
        put_nested(
          Map.fetch!(payloads, :cloudflare_nested),
          ["metadata", "response", "headers", "server_timing"],
          "edge;dur=12"
        ),
      otel_trace:
        put_nested(
          Map.fetch!(payloads, :otel_trace),
          ["resource", "telemetry", "sdk", "version"],
          "1.26.0"
        )
    }
  end

  defp wide_top_level_payload do
    integers =
      for idx <- 1..25, into: %{} do
        {"int_field_#{idx}", idx}
      end

    strings =
      for idx <- 1..25, into: %{} do
        {"string_field_#{idx}", "value_#{idx}"}
      end

    booleans =
      for idx <- 1..10, into: %{} do
        {"flag_field_#{idx}", rem(idx, 2) == 0}
      end

    %{
      "timestamp" => "2026-01-21T17:54:48.144506Z",
      "id" => "evt-wide-001",
      "event_message" => "wide top-level payload",
      "project" => "wide-top-level-project",
      "request_id" => "req-wide-001",
      "duration_ms" => 18.4,
      "tags" => Enum.map(1..8, &"tag_#{&1}"),
      "counts" => Enum.to_list(1..8),
      "items" => [
        %{"id" => 1, "status" => "ok", "duration_ms" => 12.5},
        %{"id" => 2, "status" => "warn", "error" => %{"code" => 500, "retryable" => false}}
      ],
      "nested_metrics" => %{
        "http" => %{"status_code" => 200, "method" => "POST", "retryable" => false},
        "queue" => %{"depth" => 12, "name" => "ingest", "sampled" => true}
      },
      "metadata" => %{
        "environment" => "staging",
        "service" => %{"name" => "api", "version" => "1.2.3"},
        "region" => "eu-west-1"
      }
    }
    |> Map.merge(integers)
    |> Map.merge(strings)
    |> Map.merge(booleans)
  end

  defp cloudflare_nested_payload do
    %{
      "timestamp" => "2026-01-21T17:54:48.144506Z",
      "id" => "evt-cf-001",
      "event_message" => "GET | 200 | cloudflare nested log",
      "project" => "cloudflare-project",
      "request_id" => "req-cf-001",
      "metadata" => PayloadTestUtils.standard_metadata(:cloudflare)
    }
  end

  defp otel_trace_payload do
    %{
      "timestamp" => "2026-01-21T17:54:48.144506Z",
      "id" => "evt-otel-001",
      "event_message" => "POST /functions/v1/stripe-worker",
      "project" => "otel-project",
      "metadata" => %{"type" => "span"},
      "resource" => %{
        "cloud" => %{"provider" => "aws", "region" => "us-east-1"},
        "deployment" => %{"environment" => "staging"},
        "service" => %{"name" => "supabase-api-gateway", "version" => "1.0.0"}
      },
      "scope" => %{
        "name" => "go.opentelemetry.io/contrib/instrumentation/gin",
        "version" => "0.61.0"
      },
      "attributes" => %{
        "_http_request_method" => "POST",
        "_http_route" => "/functions/v1/*path",
        "_http_status_code" => 404,
        "_network_peer_address" => "99.88.160.11",
        "_network_peer_port" => 57_785,
        "_url_path" => "/functions/v1/stripe-worker",
        "_url_scheme" => "https"
      },
      "start_time" => 1_737_482_088_144_506_000,
      "end_time" => 1_737_482_088_444_334_000,
      "severity_number" => 9,
      "retry_count" => 3,
      "span_id" => "c99f33f1bfa4fb8f",
      "trace_id" => "f9918d38f5d1cb74ec5656b9a315e5f6"
    }
  end

  defp put_nested(map, path, value) do
    put_in(map, Enum.map(path, &Access.key(&1, %{})), value)
  end
end
