defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs
  alias Logflare.Lql
  # v1 pipeline
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters

  setup do
    Logflare.Sources.Counters
    |> stub(:incriment, fn v -> v end)

    Logflare.SystemMetrics.AllLogsLogged
    |> stub(:incriment, fn v -> v end)

    # mock goth behaviour
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    :ok
  end

  describe "ingest input" do
    setup do
      [source: insert(:source, user: build(:user))]
    end

    test "empty list", %{source: source} do
      Logs
      |> Mimic.reject(:broadcast, 1)

      assert :ok = Logs.ingest_logs([], source)
    end

    test "message key gets converted to event_message", %{source: source} do
      Logs
      |> expect(:broadcast, 1, fn le ->
        assert %{"event_message" => "testing 123"} = le.body
        assert Map.keys(le.body) |> length() == 3

        le
      end)

      batch = [
        %{"message" => "testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "top level keys", %{source: source} do
      batch = [
        %{"event_message" => "testing 123", "other" => 123}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "non-map value for metadata key", %{source: source} do
      Logs
      |> expect(:broadcast, 1, fn le ->
        assert %{"metadata" => "some_value"} = le.body
        le
      end)

      batch = [
        %{"event_message" => "any", "metadata" => "some_value"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end
  end

  describe "full ingestion pipeline test" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      rls = %RecentLogsServer{source: source, source_id: source.token}
      start_supervised!(Counters)
      start_supervised!(RateCounters)
      start_supervised!({RecentLogsServer, rls})
      :timer.sleep(1000)
      [source: source]
    end

    test "additive schema update from log event", %{source: source} do
      GoogleApi.BigQuery.V2.Api.Tabledata
      |> expect(:bigquery_tabledata_insert_all, fn _conn,
                                                   _project_id,
                                                   _dataset_id,
                                                   _table_name,
                                                   opts ->
        [%{json: json}] = opts[:body].rows
        assert json["event_message"] == "testing 123"
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      GoogleApi.BigQuery.V2.Api.Tables
      |> expect(:bigquery_tables_patch, fn _conn,
                                           _project_id,
                                           _dataset_id,
                                           _table_name,
                                           [body: body] ->
        schema = body.schema
        assert %_{name: "key", type: "STRING"} = TestUtils.get_bq_field_schema(schema, "key")
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> expect(:deliver, fn _ -> :ok end)

      batch = [
        %{"event_message" => "testing 123", "key" => "value"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
      :timer.sleep(1_500)
    end
  end

  describe "ingest rules/filters" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      target = insert(:source, user: user)
      [source: source, target: target, user: user]
    end

    test "drop filter", %{user: user} do
      {:ok, lql_filters} = Lql.Parser.parse("testing", TestUtils.default_bq_schema())

      source =
        insert(:source, user: user, drop_lql_string: "testing", drop_lql_filters: lql_filters)

      Logs
      |> Mimic.reject(:broadcast, 1)

      batch = [
        %{"event_message" => "testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "no rules", %{source: source} do
      Logs
      |> expect(:broadcast, 2, fn le -> le end)

      batch = [
        %{"event_message" => "routed"},
        %{"event_message" => "routed testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "lql", %{source: source, target: target} do
      insert(:rule, lql_string: "testing", sink: target.token, source_id: source.id)
      source = source |> Repo.preload(:rules, force: true)

      Logs
      |> expect(:broadcast, 3, fn le -> le end)

      batch = [
        %{"event_message" => "not routed"},
        %{"event_message" => "testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "regex", %{source: source, target: target} do
      insert(:rule, regex: "routed123", sink: target.token, source_id: source.id)
      source = source |> Repo.preload(:rules, force: true)

      Logs
      |> expect(:broadcast, 3, fn le -> le end)

      batch = [
        %{"event_message" => "not routed"},
        %{"event_message" => "routed123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "routing depth is max 1 level", %{user: user, source: source, target: target} do
      other_target = insert(:source, user: user)
      insert(:rule, lql_string: "testing", sink: target.token, source_id: source.id)
      insert(:rule, lql_string: "testing", sink: other_target.token, source_id: target.id)
      source = source |> Repo.preload(:rules, force: true)

      Logs
      |> expect(:broadcast, 2, fn le -> le end)

      assert :ok = Logs.ingest_logs([%{"event_message" => "testing 123"}], source)
    end
  end
end
