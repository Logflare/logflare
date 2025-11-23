defmodule Logflare.Backends.UserMonitoringTest do
  use Logflare.DataCase, async: false

  require Logger

  import ExUnit.CaptureLog

  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.UserMonitoring
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.LogEvent

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
    setup do
      :ok =
        :logger.add_primary_filter(
          :user_log_intercetor,
          {&UserMonitoring.log_interceptor/2, []}
        )

      on_exit(fn ->
        :logger.remove_primary_filter(:user_log_intercetor)
      end)
    end

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
  end

  describe "system monitoring labels" do
    setup do
      start_supervised!(AllLogsLogged)
      insert(:plan)
      :ok
    end

    test "applies to backend metrics" do
      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 _opts ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user =
        insert(:user, system_monitoring: true)

      source = insert(:source, user: user, labels: "my_label=m.value")
      metrics_source = insert(:source, user: user, system_source_type: :metrics)
      start_supervised!({SourceSup, metrics_source}, id: :metrics_source)
      start_supervised!({SourceSup, source}, id: :source)

      :timer.sleep(1000)

      assert {:ok, _} = Backends.ingest_logs([%{"metadata" => %{"value" => "test"}}], source)

      TestUtils.retry_assert(fn ->
        assert [_] = Backends.list_recent_logs_local(source)

        assert [
                 _ | _
               ] = events = Backends.list_recent_logs_local(metrics_source)

        assert Enum.any?(
                 events,
                 &match?(%LogEvent{body: %{"attributes" => %{"my_label" => "test"}}}, &1)
               )

        assert Enum.any?(
                 events,
                 &match?(
                   %LogEvent{
                     body: %{"event_message" => "logflare.backends.ingest.ingested_count"}
                   },
                   &1
                 )
               )

        assert Enum.any?(
                 events,
                 &match?(
                   %LogEvent{
                     body: %{"event_message" => "logflare.backends.ingest.ingested_bytes"}
                   },
                   &1
                 )
               )
      end)
    end

    test "other users metrics" do
      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 _opts ->
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user =
        insert(:user, system_monitoring: true)

      other_user =
        insert(:user, system_monitoring: true)

      %{id: source_id} = source = insert(:source, user: user, labels: "my_label=m.value")

      %{id: other_source_id} =
        other_source = insert(:source, user: other_user, labels: "my_label=m.value")

      metrics_source = insert(:source, user: user, system_source_type: :metrics)
      other_metrics_source = insert(:source, user: other_user, system_source_type: :metrics)
      start_supervised!({SourceSup, metrics_source}, id: :metrics_source)
      start_supervised!({SourceSup, source}, id: :source)
      start_supervised!({SourceSup, other_source}, id: :other_source)
      start_supervised!({SourceSup, other_metrics_source}, id: :other_metrics_source)

      :timer.sleep(1000)

      assert {:ok, _} = Backends.ingest_logs([%{"metadata" => %{"value" => "test"}}], source)

      assert {:ok, _} =
               Backends.ingest_logs([%{"metadata" => %{"value" => "test"}}], other_source)

      TestUtils.retry_assert(fn ->
        assert [_] = Backends.list_recent_logs_local(source)
        assert [_] = Backends.list_recent_logs_local(other_source)

        assert [
                 _ | _
               ] = events = Backends.list_recent_logs_local(metrics_source)

        assert Enum.all?(
                 events,
                 &match?(
                   %LogEvent{
                     body: %{"attributes" => %{"my_label" => "test", "source_id" => ^source_id}}
                   },
                   &1
                 )
               )

        assert [
                 _ | _
               ] = events = Backends.list_recent_logs_local(other_metrics_source)

        assert Enum.all?(
                 events,
                 &match?(
                   %LogEvent{
                     body: %{
                       "attributes" => %{"my_label" => "test", "source_id" => ^other_source_id}
                     }
                   },
                   &1
                 )
               )
      end)
    end
  end

  describe "benchmark" do
    @describetag :benchmark

    setup do
      start_supervised!(AllLogsLogged)
      start_supervised!(BencheeAsync.Reporter)

      insert(:plan)

      user = insert(:user, system_monitoring: true) |> Users.preload_defaults()

      source = insert(:source, user: user)

      {:ok, user: user, source: source}
    end

    test "log interceptor", %{user: user, source: source} do
      logs_source = insert(:source, user: user, system_source_type: :logs)

      start_supervised!({SourceSup, logs_source}, id: :logs_source)

      log_event = %{level: :info, msg: {:string, "test"}, meta: %{source_id: source.id}}

      # execute once to start cache and prevent interference on statistics
      UserMonitoring.log_interceptor(log_event, nil)

      BencheeAsync.run(
        %{
          "UserMonitoring.log_interceptor/2" => fn ->
            UserMonitoring.log_interceptor(log_event, nil)
            BencheeAsync.Reporter.record()
          end
        },
        time: 3,
        warmup: 1,
        print: [configuration: false],
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    test "keep metric function", %{user: user, source: source} do
      metrics_source = insert(:source, user: user, system_source_type: :metrics)

      start_supervised!({SourceSup, metrics_source}, id: :metrics_source)

      metadata = %{"source_id" => source.id}

      # execute once to start cache and prevent interference on statistics
      UserMonitoring.keep_metric_function(metadata)

      BencheeAsync.run(
        %{
          "UserMonitoring.keep_metric_function/1" => fn ->
            UserMonitoring.keep_metric_function(metadata)
            BencheeAsync.Reporter.record()
          end
        },
        time: 3,
        warmup: 1,
        print: [configuration: false],
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    test "tags extraction" do
      metadata = %{source_id: 1, backend_id: 1, label: "123", etc: "etc"}

      BencheeAsync.run(
        %{
          "UserMonitoring.extract_tags/2" => fn ->
            UserMonitoring.extract_tags(%{}, metadata)
            BencheeAsync.Reporter.record()
          end
        },
        time: 3,
        warmup: 1,
        print: [configuration: false],
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end
end
