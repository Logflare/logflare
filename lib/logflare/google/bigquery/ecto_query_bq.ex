defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]

  def where_nested_eq(q, path, value) do
    [top_level_join, column] = split_by_dots(path)
    top_level_join = top_level_join |> String.to_atom()
    column = column |> String.to_atom()

    q
    |> join(:inner, [log], n in fragment("UNNEST(?)", field(log, ^top_level_join)))
    |> where([log, n], field(n, ^column) == ^value)
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
