defmodule Logflare.Backends.Adaptor.ClickhouseAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Source

  doctest ClickhouseAdaptor

  describe "table name generation" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      stringified_source_token =
        source.token
        |> Atom.to_string()
        |> String.replace("-", "_")

      [source: source, stringified_source_token: stringified_source_token]
    end

    test "clickhouse_ingest_table_name/1 generates a unique log ingest table name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_ingest_table_name(source) ==
               "log_events_#{stringified_source_token}"
    end

    test "clickhouse_ingest_table_name/1 will raise an exception if the table name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `log_events_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_ingest_table_name()
                   end
    end

    test "clickhouse_key_count_table_name/1 generates a unique key count table name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_key_count_table_name(source) ==
               "key_type_counts_per_min_#{stringified_source_token}"
    end

    test "clickhouse_key_count_table_name/1 will raise an exception if the table name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `key_type_counts_per_min_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_key_count_table_name()
                   end
    end

    test "clickhouse_materialized_view_name/1 generates a unique mat view name based on the source token",
         %{source: source, stringified_source_token: stringified_source_token} do
      assert ClickhouseAdaptor.clickhouse_materialized_view_name(source) ==
               "mv_key_type_counts_per_min_#{stringified_source_token}"
    end

    test "clickhouse_materialized_view_name/1 will raise an exception if the view name is equal to or exceeds 200 chars",
         %{source: source} do
      assert_raise RuntimeError,
                   ~r/^The dynamically generated ClickHouse resource name starting with `mv_key_type_counts_per_min_/,
                   fn ->
                     source
                     |> modify_source_with_long_token()
                     |> ClickhouseAdaptor.clickhouse_materialized_view_name()
                   end
    end
  end

  defp modify_source_with_long_token(%Source{} = source) do
    long_token = random_string(200) |> String.to_atom()

    %Source{
      source
      | token: long_token
    }
  end

  defp random_string(length) do
    alphanumeric = Enum.concat([?0..?9, ?a..?z])

    1..length
    |> Enum.map(fn _ -> Enum.random(alphanumeric) end)
    |> List.to_string()
  end
end
