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
  alias Logflare.Endpoints

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

  def start_otel_exporter(_context) do
    [spec] = UserMonitoring.get_otel_exporter()
    start_supervised!(spec)
    :ok
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

      TestUtils.retry_assert(fn ->
        refute capture_log(fn ->
                 Logger.info("user is monitoring", source_id: source.id)
               end) =~ "user is monitoring"

        assert Enum.any?(
                 Backends.list_recent_logs(system_source),
                 &match?(%{body: %{"event_message" => "user is monitoring"}}, &1)
               )
      end)
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

  describe "system monitoring labels" do
    setup :start_otel_exporter

    setup do
      start_supervised!(AllLogsLogged)
      insert(:plan)
      :ok
    end

    test "backends.ingest.ingested_bytes and backends.ingest.ingested_count" do
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
      pid = self()

      user =
        insert(:user, system_monitoring: true)

      other_user =
        insert(:user, system_monitoring: true)

      source = insert(:source, user: user, labels: "my_label=m.value")

      other_source = insert(:source, user: other_user, labels: "my_label=m.value")

      metrics_source = insert(:source, user: user, system_source_type: :metrics)
      other_metrics_source = insert(:source, user: other_user, system_source_type: :metrics)
      start_supervised!({SourceSup, source}, id: :source)
      start_supervised!({SourceSup, other_source}, id: :other_source)
      start_supervised!({SourceSup, metrics_source}, id: :metrics_source)
      start_supervised!({SourceSup, other_metrics_source}, id: :other_metrics_source)

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 dataset_id,
                                                 _table_name,
                                                 opts ->
        if String.starts_with?(dataset_id, "#{other_user.id}") do
          send(pid, {:insert_all, opts[:body].rows})
        end

        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      :timer.sleep(500)

      assert {:ok, _} = Backends.ingest_logs([%{"metadata" => %{"value" => "test"}}], source)

      assert {:ok, _} =
               Backends.ingest_logs([%{"metadata" => %{"value" => "different"}}], other_source)

      :timer.sleep(1500)

      source_id = source.id
      other_source_id = other_source.id
      metrics_source_id = metrics_source.id
      other_metrics_source_id = other_metrics_source.id
      assert_receive {:insert_all, [%{json: %{"attributes" => _}} | _] = rows}, 5_000

      rows = for row <- rows, do: row.json

      assert Enum.all?(rows, &match?(%{"attributes" => [%{"my_label" => "different"}]}, &1))

      for row <- rows, attr <- row["attributes"] do
        assert attr["source_id"] in [other_source_id, other_metrics_source_id]
        refute attr["source_id"] in [source_id, metrics_source_id]
        refute attr["my_label"] == "test"
      end
    end
  end

  describe "egress" do
    setup :start_otel_exporter

    setup do
      start_supervised!(AllLogsLogged)
      insert(:plan)
      :ok
    end

    test "backends.ingest.egress.request_bytes includes backend metadata" do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _, _, _, _, opts ->
        send(pid, {:insert_all, opts[:body].rows})
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      user = insert(:user, system_monitoring: true)
      metrics_source = insert(:source, user: user, system_source_type: :metrics)

      webhook_backend =
        insert(:backend,
          user: user,
          type: :webhook,
          config: %{url: "http://127.0.0.1:9999/webhook"},
          metadata: %{"environment" => "test", "region" => "us-west"}
        )

      source = insert(:source, user: user)

      start_supervised!({SourceSup, metrics_source}, id: :metrics_source)
      start_supervised!({SourceSup, source}, id: :source)

      {:ok, _} = Backends.update_source_backends(source, [webhook_backend])
      Backends.Cache.get_backend(webhook_backend.id)

      :timer.sleep(1000)

      assert {:ok, _} = Backends.ingest_logs([%{"message" => "test webhook egress"}], source)

      :timer.sleep(2500)

      assert_receive {:insert_all, [%{json: %{"attributes" => _}} | _] = rows}, 5_000

      rows = for row <- rows, do: row.json

      egress_row =
        Enum.find(
          rows,
          &match?(%{"event_message" => "logflare.backends.ingest.egress.request_bytes"}, &1)
        )

      assert egress_row, "Expected egress metric to be present"

      [attributes] = egress_row["attributes"]

      assert attributes["source_id"] == source.id
      assert attributes["backend_id"] == webhook_backend.id
      assert attributes["_backend_environment"] == "test"
      assert attributes["_backend_region"] == "us-west"
    end
  end

  describe "endpoints" do
    setup :start_otel_exporter

    setup do
      start_supervised!(AllLogsLogged)
      insert(:plan)
      :ok
    end

    test "endpoints.query.total_bytes_processed" do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Tabledata
      |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 opts ->
        send(pid, {:insert_all, opts[:body].rows})
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"result" => "1"}])}
      end)

      user = insert(:user, system_monitoring: true)
      source = insert(:source, user: user, system_source_type: :metrics)
      start_supervised!({SourceSup, source}, id: :source)
      # execute a query on the endpoint
      endpoint =
        insert(:endpoint,
          user: user,
          query: "SELECT 1",
          labels: "my_label=some_value",
          parsed_labels: %{"my_label" => "some_value"}
        )

      assert {:ok, _} = Endpoints.run_query(endpoint)
      :timer.sleep(1000)
      endpoint_id = endpoint.id

      assert_receive {:insert_all,
                      [
                        %{
                          json: %{
                            "attributes" => [
                              %{"my_label" => "some_value", "endpoint_id" => ^endpoint_id}
                            ]
                          }
                        }
                        | _
                      ]},
                     5_000
    end
  end
end
