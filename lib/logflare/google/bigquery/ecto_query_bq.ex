defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]
  alias Logflare.EctoQueryBQ.NestedPath

  def where_nested_eqs(q, pathmap) when is_list(pathmap) do
    pathmap = NestedPath.to_map(pathmap)
    where_nested_eqs(q, pathmap)
  end

  def where_nested_eqs(q, pathmap) do
    {maps, literals} = Enum.split_with(pathmap, fn {_, v} -> is_map(v) end)

    q =
      Enum.reduce(literals, q, fn {column, value}, q ->
        where(q, [..., n1], field(n1, ^column) == ^value)
      end)

    Enum.reduce(maps, q, fn {column, v}, q ->
      q
      |> join(:inner, [log, ..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
      |> where_nested_eqs(v)
    end)
  end

  def where_nested_eq(q, path, value) do
    where_nested_eqs(q, [%{path: path, value: value}])
  end

  def split_by_dots(str) do
    String.split(str, ".")
  end

  def ecto_pg_sql_to_bq_sql(sql) do
    sql
    # replaces PG-style to BQ-style positional parameters
    |> String.replace(~r/\$\d/, "?")
    # removes double quotes around the names after the dot
    |> String.replace(~r/\."([\w\.]+)"/, ".\\1")
    # removes double quotes around the qualified BQ table id
    |> String.replace(~r/FROM\s+"(.+)"/, "FROM \\1")
  end
end
