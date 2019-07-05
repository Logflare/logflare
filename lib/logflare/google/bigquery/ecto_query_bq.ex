defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]

  def where_nesteds(q, map) when is_map(map) do
    {maps, literals} = Enum.split_with(map, fn {_, v} -> is_map(v) end)

    q =
      Enum.reduce(literals, q, fn {column, {operator, value}}, q ->
        condition = build_where_condition(column, operator, value)

        where(q, ^condition)
      end)

    Enum.reduce(maps, q, fn {column, v}, q ->
      q
      |> join(:inner, [log, ..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
      |> where_nesteds(v)
    end)
  end

  def split_by_dots(str) do
    String.split(str, ".")
  end

  def build_where_condition(_column = c, _operator = op, _value = v) do
    case op do
      ">" ->
        dynamic([..., n1], field(n1, ^c) > ^v)

      ">=" ->
        dynamic([..., n1], field(n1, ^c) >= ^v)

      "<" ->
        dynamic([..., n1], field(n1, ^c) < ^v)

      "<=" ->
        dynamic([..., n1], field(n1, ^c) <= ^v)

      "=" ->
        dynamic([..., n1], field(n1, ^c) == ^v)

      "~" ->
        dynamic([..., n1], fragment("REGEXP_CONTAINS(?, ?)", field(n1, ^c), ^v))
    end
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
