defmodule Logflare.SqlTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.SQL
  alias Logflare.SqlV2
  alias Ecto.Adapters.SQL.Sandbox
  @project_id "logflare-dev-238720"
  @env "test"
  setup do
    start_supervised!(SQL)

    on_exit(fn ->
      Sandbox.unboxed_run(Logflare.Repo, fn ->
        Logflare.Repo.delete_all(Logflare.User)
        Logflare.Repo.delete_all(Logflare.Source)
      end)
    end)

    :ok
  end

  test "transform table names" do
    Sandbox.unboxed_run(Logflare.Repo, fn ->
      user = insert(:user)
      source = insert(:source, user: user, name: "my_table")
      query = "select val, my_table.abc from my_table where my_table.val > 5 group by my_table.val order by my_table.val"
      table = bq_table_name(source)
      assert {:ok, v1} = SQL.transform(query, user)

      assert {:ok, v2} = SqlV2.transform(query, user)
      assert String.downcase(v1) == String.downcase(v2)

      assert String.downcase(v2) == "select val, #{table}.abc from #{table} where #{table}.val > 5 group by #{table}.val order by #{table}.val"
    end)
  end

  test "transform table names backquoted" do
    Sandbox.unboxed_run(Logflare.Repo, fn ->
      user = insert(:user)
      source = insert(:source, user: user, name: "my.table")
      query = "with src as (select n from `my.table`) select n from src"
      table = bq_table_name(source)
      assert {:ok, v1} = SQL.transform(query, user)
      assert {:ok, v2} = SqlV2.transform(query, user)
      assert String.downcase(v1) == String.downcase(v2)
      assert String.downcase(v2) == "with src as (select n from #{table}) select n from src"
    end)
  end



  test "transform table names substitution in CTEs" do
    Sandbox.unboxed_run(Logflare.Repo, fn ->
      user = insert(:user)
      source = insert(:source, user: user, name: "my.table")
      query = "select val from `my.table` where `my.table`.val > 5"
      table = bq_table_name(source)
      assert {:ok, v1} = SQL.transform(query, user)
      assert {:ok, v2} = SqlV2.transform(query, user)
      assert String.downcase(v1) == String.downcase(v2)
      assert String.downcase(v2) == "select val from #{table} where #{table}.val > 5"
    end)
  end

  defp bq_table_name(%{user: user} = source) do
    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    "`#{@project_id}.#{user.id}_#{@env}.#{token}`"
  end
end
