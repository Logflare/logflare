defmodule Logflare.Logs.SearchTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.User.BigQueryUDFs
  alias Logflare.Google.BigQuery.GenUtils
  use Logflare.DataCase, async: true
  import Logflare.Factory
  alias Logflare.Source.RecentLogsServer, as: RLS
  @test_token :"2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

  setup_all do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)

    source = Sources.get_by_and_preload(token: @test_token)
    user = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    Sources.Cache.put_bq_schema(@test_token, table_schema())

    {:ok, user} = BigQueryUDFs.create_if_not_exists_udfs_for_user_dataset(user)

    {:ok, sources: [source], users: [user]}
  end

  describe "search events" do
    test "search for source and regex events", %{sources: [source | _], users: [_user | _]} do
      search_op = %SO{
        source: source,
        querystring: ~S|"x[123] \d\d1"|,
        chart_aggregate: :count,
        chart_period: :minute,
        tailing?: false,
        tailing_initial?: false
      }

      {_, %{rows: rows} = so} = Search.search_events(search_op)

      assert so.error == nil
      assert length(rows) == 0
    end
  end

  describe "search aggregates tailing" do
    setup context do
      [source | _] = context.sources

      so0 = %SO{
        source: source,
        querystring: ~S|"x[123] \d\d1"|,
        chart_aggregate: :count,
        chart_period: :minute,
        tailing?: true
      }

      {:ok, Map.merge(context, %{so: so0})}
    end

    test "returns correct response shapes", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = %{so0 | chart_period: :second}
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert %{timestamp: _, value: _, datetime: _} = hd(rows)
    end

    test "with default second chart period ", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = %{so0 | chart_period: :second}
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 180
    end

    test "with default minute chart period ", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = so0
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 120

      # assert length(rows) == [%{}]
    end

    test "with default hour chart period ", %{sources: [source | _], users: [_user | _], so: so0} do
      so = %{so0 | chart_period: :hour}

      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 168
    end

    test "with default day chart period ", %{sources: [source | _], users: [_user | _], so: so0} do
      so = %{so0 | chart_period: :day}

      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)
      assert so.error == nil

      assert length(rows) == 31
      # assert length(rows) == [%{}]
    end

    test "search aggregates with chart operator", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      # assert length(rows) == [%{}]

      so = %{so0 | chart_period: :minute, querystring: "chart:metadata.int_field_1"}

      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 120
    end
  end

  describe "search aggregates tailing false" do
    setup context do
      [source | _] = context.sources

      so0 = %SO{
        source: source,
        querystring: ~S|"x[123] \d\d1"|,
        chart_aggregate: :count,
        chart_period: :minute,
        tailing?: false
      }

      {:ok, Map.merge(context, %{so: so0})}
    end

    test "returns correct response shapes", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = %{so0 | chart_period: :second}
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert %{timestamp: _, value: _, datetime: _} = hd(rows)
    end

    test "with default minute chart period", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = %{so0 | querystring: "t:2020-01-01..2020-01-02"}
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 60 * 24 * 2
    end

    test "with default minute chart period 2", %{
      sources: [source | _],
      users: [_user | _],
      so: so0
    } do
      so = %{so0 | querystring: "t:2020-01-01T00:00:00Z..2020-01-01T15:00:00Z"}
      {_, %{rows: rows} = so} = Search.search_result_aggregates(so)

      assert so.error == nil
      assert length(rows) == 15 * 60 + 1
    end
  end

  def table_schema() do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "id",
          policyTags: nil,
          type: "STRING"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "float_field_1",
              policyTags: nil,
              type: "FLOAT"
            },
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              categories: nil,
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "int_field_1",
              policyTags: nil,
              type: "INTEGER"
            }
          ],
          mode: "REPEATED",
          name: "metadata",
          policyTags: nil,
          type: "RECORD"
        },
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          categories: nil,
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          policyTags: nil,
          type: "TIMESTAMP"
        }
      ]
    }
  end
end
