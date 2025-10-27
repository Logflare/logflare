defmodule Logflare.Logs.SearchQueriesTest do
  use Logflare.DataCase, async: false

  import Ecto.Query

  alias Logflare.Logs.SearchQueries

  describe "source_log_event_by_path/3" do
    test "builds query for top-level field" do
      bq_table_id = "test_table"
      path = "event_message"
      value = "test message"

      query = SearchQueries.source_log_event_by_path(bq_table_id, path, value)

      assert %Ecto.Query{} = query
      assert query.from.source == {"test_table", nil}

      [where_clause] = query.wheres
      assert where_clause.op == :and
    end

    test "builds query for nested field" do
      bq_table_id = "test_table"
      path = "metadata.user.id"
      value = "user123"

      query = SearchQueries.source_log_event_by_path(bq_table_id, path, value)

      assert %Ecto.Query{} = query
      assert query.from.source == {"test_table", nil}

      # Verify joins are added for nested fields
      assert length(query.joins) == 2
    end

    test "uses string field names without atom conversion" do
      bq_table_id = "test_table"
      path = "custom.field.name"
      value = "test_value"

      assert %Ecto.Query{
               joins: [_, _]
             } = SearchQueries.source_log_event_by_path(bq_table_id, path, value)
    end

    test "extracts last column name as string" do
      bq_table_id = "test_table"
      path = "some.nested.field"
      value = "test"

      assert %Ecto.Query{} = SearchQueries.source_log_event_by_path(bq_table_id, path, value)
    end
  end

  describe "select_merge_agg_value/3" do
    test "count aggregation for timestamp" do
      base_query = from("test_table")

      assert %Ecto.Query{select: select} =
               SearchQueries.select_merge_agg_value(base_query, :count, :timestamp)

      assert select != nil
    end
  end

  describe "select_merge_agg_value/4" do
    test "sum aggregation for base table" do
      base_query = from("test_table")

      query = SearchQueries.select_merge_agg_value(base_query, :sum, "custom_field", :base_table)

      assert %Ecto.Query{} = query

      # Verify select clause is added
      assert query.select != nil
    end

    test "avg aggregation for joined table" do
      base_query = from("test_table")

      query =
        SearchQueries.select_merge_agg_value(base_query, :avg, "response_time", :joined_table)

      assert %Ecto.Query{} = query

      # Verify select clause is added
      assert query.select != nil
    end

    test "works with string field names" do
      base_query = from("test_table")

      # Test that string field names work without atom conversion
      query =
        SearchQueries.select_merge_agg_value(base_query, :max, "dynamic.field.name", :base_table)

      assert %Ecto.Query{} = query

      # Verify select clause is added
      assert query.select != nil
    end
  end

  describe "source_log_event_query/3" do
    test "builds query with id and timestamp" do
      bq_table_id = "test_table"
      id = "test_id_123"
      timestamp = ~U[2023-01-01 12:00:00Z]

      query = SearchQueries.source_log_event_query(bq_table_id, id, timestamp)

      assert %Ecto.Query{wheres: [_ | _]} = query
    end
  end

  describe "source_log_event_id/2" do
    test "builds query for specific log ID" do
      bq_table_id = "test_table"
      id = "specific_log_id"

      query = SearchQueries.source_log_event_id(bq_table_id, id)

      assert %Ecto.Query{} = query

      # Verify where clause for id
      assert length(query.wheres) == 1
    end
  end

  describe "where_partitiondate_between/3" do
    test "adds partition date filter" do
      base_query = from("test_table")
      min_date = ~U[2023-01-01 00:00:00Z]
      max_date = ~U[2023-01-31 23:59:59Z]

      query = SearchQueries.where_partitiondate_between(base_query, min_date, max_date)

      assert %Ecto.Query{} = query

      # Verify where clause is added
      assert length(query.wheres) == 1
    end
  end

  describe "limit_aggregate_chart_period/2" do
    test "adds limit based on period" do
      base_query = from("test_table")

      query = SearchQueries.limit_aggregate_chart_period(base_query, :day)

      assert %Ecto.Query{} = query

      # Verify limit clause is added
      assert query.limit != nil
    end

    test "different periods add limit clause" do
      base_query = from("test_table")

      hour_query = SearchQueries.limit_aggregate_chart_period(base_query, :hour)
      minute_query = SearchQueries.limit_aggregate_chart_period(base_query, :minute)
      second_query = SearchQueries.limit_aggregate_chart_period(base_query, :second)

      # Verify all queries have limit clauses
      assert hour_query.limit != nil
      assert minute_query.limit != nil
      assert second_query.limit != nil
    end
  end
end
