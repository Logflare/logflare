defmodule Logflare.Backends.UserMonitoringTest do
  use Logflare.DataCase, async: false

  require Logger

  import ExUnit.CaptureLog

  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Telemetry

  def source_and_user(_context) do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    source_b = insert(:source, user: user)

    start_supervised!({SourceSup, source}, id: :source)
    start_supervised!({SourceSup, source_b}, id: :source_b)

    :timer.sleep(250)
    [source: source, source_b: source_b, user: user]
  end


  describe "logs" do
    setup :source_and_user

    test "are routed to user's system source when monitoring is on", %{user: user, source: source} do
      {:ok, user} = Users.update_user_allowed(user, %{system_monitoring: true})
      system_source = Sources.get_by(user_id: user.id, system_source_type: :logs)

      # Non-user-specific logs goes to the default logger backends
      assert capture_log(fn -> Logger.info("common log") end) =~ "common log"

      # User-specfic logs are routed to users with system monitoring on

      refute capture_log(fn ->
               Logger.info("user is monitoring", source_id: source.id)
             end) =~ "user is monitoring"

      assert Enum.any?(
               Backends.list_recent_logs(system_source),
               &match?(%{body: %{"event_message" => "user is monitoring"}}, &1)
             )
    end

    test "are not routed to user's system source when not monitoring", %{
      user: user,
      source: source
    } do
      {:ok, user} = Users.update_user_allowed(user, %{system_monitoring: true})
      Users.update_user_allowed(user, %{system_monitoring: false})
      system_source = Sources.get_by(user_id: user.id, system_source_type: :logs)

      assert capture_log(fn ->
               Logger.info("user not monitoring", source_id: source.id)
             end) =~ "user not monitoring"

      refute Enum.any?(
               Backends.list_recent_logs(system_source),
               &match?(%{body: %{"event_message" => "user not monitoring"}}, &1)
             )
    end

    setup do
      :ok =
        :logger.add_primary_filter(
          :user_log_intercetor,
          {&Logflare.Backends.UserMonitoring.log_interceptor/2, []}
        )

      on_exit(fn ->
        :logger.remove_primary_filter(:user_log_intercetor)
      end)
    end
  end

  describe "metrics" do
    setup do
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

    test "are routed to user's system source when flag is true", %{
      user_1: user,
      backend_1: %{id: backend_id}
    } do
      user |> Users.update_user_allowed(%{system_monitoring: true})
      metadata = %{backend_id: backend_id}

      # main exporter's metric keep function returns false
      refute Telemetry.keep_metric_function(metadata)

      # user exporter keep user specific metrics, and only that

      system_source =
        Sources.get_by(user_id: user.id, system_source_type: :metrics)

      start_supervised!({SourceSup, system_source})

      :telemetry.execute([:logflare, :test, :generic_metric], %{value: 123})
      :telemetry.execute([:logflare, :test, :user_specific], %{value: 456}, metadata)

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
      user_1: user,
      backend_1: %{id: backend_id}
    } do
      metadata = %{backend_id: backend_id}

      # main exporter's metric keep function returns true
      assert Telemetry.keep_metric_function(metadata)

      # if SourceSup is up, it stil won't ingest any metric
      system_source =
        user.id
        |> Sources.create_user_system_sources()
        |> Enum.find(&(&1.system_source_type == :metrics))

      start_supervised!({SourceSup, system_source})

      :telemetry.execute([:logflare, :test, :user_specific], %{value: 456}, metadata)

      user_exporter_metrics =
        OtelMetricExporter.MetricStore.get_metrics(:"system.metrics-#{user.id}")

      refute match?(
               %{
                 {:last_value, "logflare.test.user_specific.value"} => %{
                   %{backend_id: ^backend_id} => 456
                 }
               },
               user_exporter_metrics
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
