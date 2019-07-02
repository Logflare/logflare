defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]

  def where_nested_eq(q, path, value) do
    paths = split_by_dots(path)
    apply_nested_operator(q, paths, value, &==/2)
  end

  def apply_nested_eq_operator(q, [join, column], value) do
    column = column |> String.to_atom()
    join = join |> String.to_atom()

    q
    |> join(:inner, [log], n in fragment("UNNEST(?)", field(log, ^join)))
    |> where([log, ..., n1], field(n1, ^column) == ^value)
  end

  def apply_nested_operator(q, path, value, operator) when is_list(path) do
    [join | rest] = path
    join = join |> String.to_atom()
    q = join(q, :inner, [log], n in fragment("UNNEST(?)", field(log, ^join)))
    apply_nested_operator(q, rest, value, operator)
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
