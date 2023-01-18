defmodule Logflare.SqlTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.SqlV2
  @project_id "logflare-dev-238720"
  @env "test"

  test "transforms table names correctly" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")
    source_dots = insert(:source, user: user, name: "my.table.name")
    source_other = insert(:source, user: user, name: "other_table")
    table = bq_table_name(source)
    table_other = bq_table_name(source_other)
    table_dots = bq_table_name(source_dots)

    for {input, expected} <- [
          # quoted
          {"select val from `my_table` where `my_table`.val > 5",
           "select val from #{table} where #{table}.val > 5"},
          #  source names with dots
          {"select val from `my.table.name` where `my.table.name`.val > 5",
           "select val from #{table_dots} where #{table_dots}.val > 5"},
          # joins
          {"select a from my_table join other_table as f on a = 123",
           "select a from #{table} join #{table_other} as f on a = 123"},
          # cross join + unnest with no join condition
          {"select a from my_table cross join unnest(my_table.col) as f",
           "select a from #{table} cross join unnest(#{table}.col) as f"},
          #  inner join  + unnest with join condition
          {"select a from my_table join unnest(my_table.col) on true",
           "select a from #{table} join unnest(#{table}.col) on true"},
          # where
          {"select val from my_table where my_table.val > 5",
           "select val from #{table} where #{table}.val > 5"},
          # select named column
          {"select val, my_table.abc from my_table", "select val, #{table}.abc from #{table}"},
          # group by
          {"select val from my_table group by my_table.val",
           "select val from #{table} group by #{table}.val"},
          #  order by
          {"select val from my_table order by my_table.val",
           "select val from #{table} order by #{table}.val"},
          #  CTE
          {"with src as (select n from `my_table`) select n from src",
           "with src as (select n from #{table}) select n from src"},
          # having
          {"select val from my_table group by my_table.abc having count(my_table.id) > 5",
           "select val from #{table} group by #{table}.abc having count(#{table}.id) > 5"},
          # alias
          {"select a from my_table as src where src.b > 5",
           "select a from #{table} as src where src.b > 5"},
          # joins
          {"select a from my_table left join other_table on my_table.d = other_table.e",
           "select a from #{table} left join #{table_other} on #{table}.d = #{table_other}.e"},
          # CTE with union
          {"with abc as (select val from my_table where val > 5) select val from abc union select a from other_table",
           "with abc as (select val from #{table} where val > 5) select val from abc union select a from #{table_other}"},
          # recursive CTE
          {"with src as (select a from my_table union select a from src) select a from src",
           "with src as (select a from #{table} union select a from src) select a from src"},
          # CTE referencing
          {
            "with src as (select a from my_table), abc as (select b from src) select c from abc union select a from src",
            "with src as (select a from #{table}), abc as (select b from src) select c from abc union select a from src"
          },
          # sandboxed queries
          {
            {"with src as (select a from my_table), src2 as (select a from src where a > 5) select c from src",
             "select a, b, c from src2"},
            "with src as (select a from #{table}), src2 as (select a from src where a > 5) select a, b, c from src2"
          }
        ] do
      assert {:ok, v2} = SqlV2.transform(input, user)
      assert String.downcase(v2) == expected
      assert {:ok, v2} = SqlV2.transform(input, user.id)
      assert String.downcase(v2) == expected
    end

    # queries where v1 differs from v2, don't test for equality
    for {input, expected} <- [
          # subquery
          {"select a from (select b from my_table)", "select a from (select b from #{table})"}
        ] do
      assert {:ok, v2} = SqlV2.transform(input, user)
      assert String.downcase(v2) == expected
    end

    # invalid queries
    for {input, expected} <- [
          # select-into queries are not parsed.
          {"SELECT a FROM a INTO b", "end of statement"},
          {
            {"with src as (select a from my_table) select c from src",
             "select a from src into src"},
            "end of statement"
          },
          # block no wildcard select
          {"select * from a", "restricted wildcard"},
          {"SELECT a.* FROM a", "restricted wildcard"},
          {"SELECT q, a.* FROM a", "restricted wildcard"},
          {"SELECT a FROM (SELECT * FROM a)", "restricted wildcard"},
          {"WITH q AS (SELECT a FROM a) SELECT * FROM q", "restricted wildcard"},
          {"SELECT a FROM a UNION ALL SELECT * FROM b", "restricted wildcard"},
          {
            {"with src as (select a from my_table) select c from src", "select * from src"},
            "restricted wildcard"
          },
          # sandbox: restricted table references not in CTE
          {
            {"with src as (select a from my_table) select c from src", "select a from my_table"},
            "Table not found in CTE: (my_table)"
          },
          # sandbox: restricted functions
          {"SELECT SESSION_USER()", "Restricted function session_user"},
          {"SELECT EXTERNAL_QUERY('','')", "Restricted function external_query"},
          {
            {"with src as (select a from my_table) select c from src", "select session_user()"},
            "Restricted function session_user"
          },
          {
            {"with src as (select a from my_table) select c from src",
             "select external_query('','')"},
            "Restricted function external_query"
          },
          # block DML
          # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax
          {
            "insert a (x,y) values ('test', 5)",
            "Only SELECT queries allowed"
          },
          {
            "update a set x = 1",
            "Only SELECT queries allowed"
          },
          {
            "delete from a where x = 1",
            "Only SELECT queries allowed"
          },
          {
            "truncate table a",
            "Only SELECT queries allowed"
          },
          {
            "MERGE t USING s ON t.product = s.product
              WHEN MATCHED THEN
                UPDATE SET quantity = t.quantity + s.quantity
              WHEN NOT MATCHED THEN
                INSERT (product, quantity) VALUES(product, quantity) ",
            "Only SELECT queries allowed"
          },
          {
            "drop table a",
            "Only SELECT queries allowed"
          },
          {{"with src as (select a from my_table) select c from src", "update a set x=2"},
           "Only SELECT queries allowed"},
          {{"with src as (select a from my_table) select c from src", "drop table a"},
           "Only SELECT queries allowed"},
          #  Block multiple queries
          {
            "select a from b; select c from d;",
            "Only singular query allowed"
          },
          {{"with src as (select a from my_table) select c from src",
            "select a from b; select c from d;"}, "Only singular query allowed"},
          # no source name in query
          {"select datetime() from light-two-os-directions-test",
           "can't find source light-two-os-directions-test"},
          {"select datetime() from `light-two-os-directions-test`",
           "can't find source light-two-os-directions-test"},
          {"with src as (select a from unknown_table) select datetime() from my_table",
           "can't find source unknown_table"}
        ] do
      assert {:error, err2} = SqlV2.transform(input, user)
      assert err2 =~ expected
    end
  end

  test "sources/2 creates a source mapping present for sources present in the query" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")
    other_source = insert(:source, user: user, name: "other.table")
    input = "select a from my_table"
    expected = %{"my_table" => Atom.to_string(source.token)}
    assert {:ok, ^expected} = SqlV2.sources(input, user)

    input = "select a from my_table, `other.table`"

    expected = %{
      "my_table" => Atom.to_string(source.token),
      "other.table" => Atom.to_string(other_source.token)
    }

    assert {:ok, ^expected} = SqlV2.sources(input, user)
  end

  test "source_mapping/3 updates an SQL string with renamed sources" do
    user = insert(:user)
    source = insert(:source, user: user, name: "my_table")

    mapping = %{
      "old" => Atom.to_string(source.token)
    }

    input = "select old.a from old"
    expected = "select new.a from new"

    Ecto.Changeset.change(source, name: "new")
    |> Logflare.Repo.update()

    assert {:ok, output} = SqlV2.source_mapping(input, user.id, mapping)
    assert String.downcase(output) == expected
    assert {:ok, output} = SqlV2.source_mapping(input, user, mapping)
    assert String.downcase(output) == expected
  end

  test "parameters/1 extracts out parameter names in the SQL string" do
    for {input, output} <- [
          {"select old.a from old where @test = 123", ["test"]},
          {"select @a from old", ["a"]},
          {"select old.a from old where char_length(@c)", ["c"]},
          {"with q as (select old.a from old where char_length(@c)) select 1", ["c"]},
          {"with q as (select @c from old) select 1", ["c"]}
        ] do
      assert {:ok, ^output} = SqlV2.parameters(input)
      assert {:ok, ^output} = SqlV2.parameters(input)
    end
  end

  defp bq_table_name(%{user: user} = source) do
    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    "`#{@project_id}.#{user.id}_#{@env}.#{token}`"
  end
end
