defmodule Logflare.Sql.ClickHouseSqlSettingsTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Sources
  alias Logflare.Sql
  alias Logflare.User

  setup do
    stub(Sources, :list_sources_by_user, fn %User{} -> [] end)
    %{user: %User{id: 123}}
  end

  test "transformation admits only typed outer requests without resolving execution policy", %{
    user: user
  } do
    for {key, value} <- [
          {"max_execution_time", "0.25"},
          {"max_memory_usage", "1024"},
          {"max_rows_to_read", "100"},
          {"max_bytes_to_read", "1024"},
          {"max_threads", "2"},
          {"priority", "10"}
        ],
        prefix <- ["", "EXPLAIN "] do
      assert {:ok, sql} =
               transform("#{prefix}SELECT 1 AS a SETTINGS #{key} = #{value}", user)

      assert sql =~ "#{key} = #{value}"
    end
  end

  test "transformation rejects invalid, duplicate and nested lower-only settings", %{user: user} do
    for query <- [
          "SELECT 1 AS a SETTINGS max_threads = 0",
          "SELECT 1 AS a SETTINGS max_threads = '2'",
          "SELECT 1 AS a SETTINGS max_execution_time = -1",
          "SELECT 1 AS a SETTINGS priority = 1.5",
          "SELECT 1 AS a SETTINGS max_threads = 2, MAX_THREADS = 3",
          "WITH src AS (SELECT 1 AS a SETTINGS max_threads = 1) SELECT a FROM src",
          "SELECT a FROM (SELECT 1 AS a SETTINGS priority = 10)"
        ] do
      assert {:error, _reason} = transform(query, user)
    end
  end

  test "new request keys do not expand the ordinary settings allowlist", %{user: user} do
    assert {:error, "Restricted setting allow_introspection_functions"} =
             transform(
               "SELECT 1 AS a SETTINGS max_threads = 1, allow_introspection_functions = 1",
               user
             )

    assert {:ok, _sql} =
             transform("SELECT 1 AS a SETTINGS optimize_read_in_order = 1", user)
  end

  test "referencing an endpoint with SQL requests fails rather than changing their scope", %{
    user: user
  } do
    endpoint = %EndpointQuery{
      name: "limited",
      language: :ch_sql,
      query: "SELECT 1 AS a SETTINGS max_threads = 1"
    }

    assert {:ok, expanded} = Sql.expand_subqueries(:ch_sql, "SELECT a FROM limited", [endpoint])
    assert {:error, reason} = transform(expanded, user)
    assert reason =~ "only allowed in the outer SETTINGS clause"
  end

  test "sandbox assembly rejects requests on either side rather than dropping or nesting them", %{
    user: user
  } do
    base = "WITH src AS (SELECT 1 AS a) SELECT a FROM src"
    consumer = "SELECT a FROM src"

    for input <- [
          {base <> " SETTINGS max_threads = 1", consumer},
          {base, consumer <> " SETTINGS priority = 10"}
        ] do
      assert {:error, reason} = transform(input, user)
      assert reason =~ "not supported in sandboxed SQL"
    end

    assert {:ok, _sql} =
             transform(
               {base, consumer <> " SETTINGS optimize_read_in_order = 1"},
               user
             )
  end

  test "non-endpoint SQL transformation still rejects resource and priority settings", %{
    user: user
  } do
    for key <-
          ~w(max_execution_time max_memory_usage max_rows_to_read max_bytes_to_read max_threads priority) do
      expected = "Restricted setting #{key}"

      assert {:error, ^expected} =
               Sql.transform(:ch_sql, "SELECT 1 AS a SETTINGS #{key} = 1", user)
    end
  end

  @spec transform(String.t() | {String.t(), String.t()}, User.t()) ::
          {:ok, String.t()} | {:error, String.t()}
  defp transform(input, user),
    do: Sql.transform(:ch_sql, input, user, allow_clickhouse_sql_settings: true)
end
