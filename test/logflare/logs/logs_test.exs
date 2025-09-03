defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Logs
  alias Logflare.Lql
  alias Logflare.Sources.Source.V1SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  def source_and_user(_context) do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    source_b = insert(:source, user: user)

    start_supervised!({V1SourceSup, source: source}, id: :source)
    start_supervised!({V1SourceSup, source: source_b}, id: :source_b)

    :timer.sleep(250)
    [source: source, source_b: source_b, user: user]
  end

  setup :source_and_user

  describe "ingest input" do
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
    test "additive schema update from log event", %{source: source} do
      GoogleApi.BigQuery.V2.Api.Tabledata
      |> expect(:bigquery_tabledata_insert_all, fn conn,
                                                   _project_id,
                                                   _dataset_id,
                                                   _table_name,
                                                   opts ->
        assert {Tesla.Adapter.Finch, :call, [kw]} = conn.adapter
        assert kw[:name] == Logflare.FinchIngest

        [%{json: json}] = opts[:body].rows
        assert json["event_message"] == "testing 123"
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      pid = self()

      GoogleApi.BigQuery.V2.Api.Tables
      |> expect(:bigquery_tables_patch, fn conn,
                                           _project_id,
                                           _dataset_id,
                                           _table_name,
                                           [body: body] ->
        #  use default config adapter
        assert conn.adapter == nil
        schema = body.schema
        assert %_{name: "key", type: "STRING"} = TestUtils.get_bq_field_schema(schema, "key")
        # send msg to main test proc that mock was correctly called.
        send(pid, :ok)
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> expect(:deliver, fn _ -> :ok end)

      batch = [
        %{"event_message" => "testing 123", "key" => "value"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
      assert_receive :ok, 1_500
      :timer.sleep(1_500)
    end
  end

  describe "ingest rules/filters" do
    test "drop filter", %{user: user} do
      {:ok, lql_filters} = Lql.Parser.parse("testing", TestUtils.default_bq_schema())

      drop_test =
        insert(:source, user: user, drop_lql_string: "testing", drop_lql_filters: lql_filters)

      Logs
      |> Mimic.reject(:broadcast, 1)

      batch = [
        %{"event_message" => "testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, drop_test)
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

    test "lql", %{source: source, source_b: target} do
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

    test "rule without sink", %{source: source} do
      insert(:rule, lql_string: "testing", sink: nil, source_id: source.id)
      source = source |> Repo.preload(:rules, force: true)

      Logs
      |> expect(:broadcast, 1, fn le -> le end)

      batch = [
        %{"event_message" => "testing 123"}
      ]

      assert :ok = Logs.ingest_logs(batch, source)
    end

    test "routing depth is max 1 level", %{user: user, source: source, source_b: target} do
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
