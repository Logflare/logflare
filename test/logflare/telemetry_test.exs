defmodule Logflare.TelemetryTest do
  use Logflare.DataCase, async: false

  alias Logflare.Telemetry
  alias Logflare.TestUtils
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Backends.SourceSup

  describe "process metrics" do
    test "retrieves and emits top 10 by memory" do
      event = [:logflare, :system, :top_processes, :memory]
      TestUtils.attach_forwarder(event)
      Telemetry.process_memory_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{size: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 10 by message queue" do
      event = [:logflare, :system, :top_processes, :message_queue]
      TestUtils.attach_forwarder(event)
      Telemetry.process_message_queue_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{length: _}, metrics)
      assert match?(%{name: _}, meta)
    end
  end

  describe "ets_table_metrics/1" do
    test "retrieves and emits top 10 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "retrieves and emits top 100 by memory usage" do
      event = [:logflare, :system, :top_ets_tables, :grouped]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      assert_receive {:telemetry_event, ^event, metrics, meta}
      assert match?(%{memory: _}, metrics)
      assert match?(%{name: _}, meta)
    end

    test "ignores tables that were deleted during listing" do
      # simulates all tables being deleted to simplify testing, so we can
      # just check if all tables were skipped for returning :undefined
      Logflare.Utils
      |> stub(:ets_info, fn _ -> :undefined end)

      event = [:logflare, :system, :top_ets_tables, :individual]
      TestUtils.attach_forwarder(event)
      Telemetry.ets_table_metrics()

      refute_receive {:telemetry_event, ^event, _, _}
    end
  end

  describe "user specific metrics" do
    setup do
      setup_otel()
      start_supervised!(Telemetry)

      insert(:plan)

      user_1 = insert(:user)
      user_1 = Users.preload_defaults(user_1)

      backend_1 =
        insert(:backend,
          user: user_1,
          type: :webhook,
          config: %{url: "http://test.com"},
          default_ingest?: false
        )

      user_2 = insert(:user)
      user_2 = Users.preload_defaults(user_2)

      backend_2 =
        insert(:backend,
          user: user_2,
          type: :webhook,
          config: %{url: "http://test.com"},
          default_ingest?: false
        )

      {:ok, user_1: user_1, user_2: user_2, backend_1: backend_1, backend_2: backend_2}
    end

    defp setup_otel do
      original_logflare_env = Application.get_all_env(:logflare)

      Application.put_env(:logflare, :opentelemetry_enabled?, true)

      original_otel_env = Application.get_all_env(:opentelemetry)

      new_otel_env =
        original_otel_env
        |> Keyword.merge(
          resource: ["service.cluster": :test],
          sdk_disabled: false,
          traces_exporter: :otlp,
          sampler:
            {:parent_based,
             %{
               root:
                 {LogflareWeb.OpenTelemetrySampler,
                  %{
                    probability:
                      System.get_env("LOGFLARE_OTEL_SAMPLE_RATIO", "1.0")
                      |> String.to_float()
                  }}
             }}
        )

      original_otel_exporter_env = Application.get_all_env(:opentelemetry_exporter)

      new_otel_exporter_env =
        original_otel_exporter_env
        |> Keyword.merge(
          otlp_protocol: :http_protobuf,
          otlp_endpoint: "",
          otlp_compression: :gzip,
          otlp_headers: []
        )

      Application.put_all_env(
        opentelemetry: new_otel_env,
        opentelemetry_exporter: new_otel_exporter_env
      )

      on_exit(fn ->
        Application.put_all_env(
          logflare: original_logflare_env,
          opentelemetry: original_otel_env,
          opentelemetry_exporter: original_otel_exporter_env
        )
      end)
    end

    test "are routed to user's system source when flag is true", %{
      user_1: user,
      backend_1: %{id: backend_id}
    } do
      user |> Users.update_user_allowed(%{system_monitoring: true})

      system_source =
        Sources.get_by(user_id: user.id, system_source_type: :metrics)

      start_supervised!({SourceSup, system_source})

      :telemetry.execute([:logflare, :test, :generic_metric], %{value: 123})

      :telemetry.execute([:logflare, :test, :user_specific], %{value: 456}, %{
        backend_id: backend_id
      })

      # main exporter don't keep user specific metric
      main_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:otel_metric_exporter)

      refute match?(
               %{{:last_value, "logflare.test.user_specific.value"} => _},
               main_exporter_metrics
             )

      # user exporter keep user specific metric

      user_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:"system.metrics-#{user.id}")

      refute match?(
               %{{:last_value, "logflare.test.generic_metric.value"} => _},
               user_exporter_metrics
             )

      assert match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_id} => 456
                 }
               },
               user_exporter_metrics
             )
    end

    test "stay on main exporter when flag is false", %{
      backend_1: %{id: backend_id}
    } do
      :telemetry.execute([:logflare, :test, :user_specific], %{value: 456}, %{
        backend_id: backend_id
      })

      # main exporter keep that metric
      main_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:otel_metric_exporter)

      assert match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_id} => 456
                 }
               },
               main_exporter_metrics
             )
    end

    test "dont get mixed between users", %{
      user_1: user_1,
      user_2: user_2,
      backend_1: %{id: backend_1_id},
      backend_2: %{id: backend_2_id}
    } do
      # user 1 setup
      user_1 |> Users.update_user_allowed(%{system_monitoring: true})

      system_source_1 =
        Sources.get_by(user_id: user_1.id, system_source_type: :metrics)

      start_supervised!({SourceSup, system_source_1}, id: {:source_sup, 1})

      # user 2 setup

      user_2 |> Users.update_user_allowed(%{system_monitoring: true})

      system_source_2 =
        Sources.get_by(user_id: user_2.id, system_source_type: :metrics)

      start_supervised!({SourceSup, system_source_2}, id: {:source_sup, 2})

      # sending signals related to both users

      :telemetry.execute([:logflare, :test, :user_specific], %{value: 123}, %{
        backend_id: backend_1_id
      })

      :telemetry.execute([:logflare, :test, :user_specific], %{value: 456}, %{
        backend_id: backend_2_id
      })

      user_1_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:"system.metrics-#{user_1.id}")

      assert match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_1_id} => 123
                 }
               },
               user_1_exporter_metrics
             )

      refute match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_2_id} => _
                 }
               },
               user_1_exporter_metrics
             )

      user_2_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:"system.metrics-#{user_2.id}")

      assert match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_2_id} => 456
                 }
               },
               user_2_exporter_metrics
             )

      refute match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_1_id} => _
                 }
               },
               user_2_exporter_metrics
             )
    end
  end
end
