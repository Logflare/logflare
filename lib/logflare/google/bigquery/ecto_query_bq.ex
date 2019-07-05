defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query
  import Ecto.Adapters.SQL, only: [to_sql: 3]

  def where_nesteds(q, pathvalops) do
    pathvalops
    |> group_by_nested_column_path()
    |> Enum.reduce(q, fn {path, colvalops}, q ->
      where_nested(q, path, colvalops)
    end)
  end

  def where_nested(q, path, colvalops) when is_list(colvalops) do
    q = path
      |> split_by_dots()
      |> List.wrap()
      |> Enum.reduce(%{q: q, level: 0}, fn column, acc ->
      column = String.to_atom(column)
      level = acc.level + 1
      q = case level do
        1 ->
          join(acc.q, :inner, [log], n in fragment("UNNEST(?)", field(log, ^column)))

        _ ->
          join(acc.q, :inner, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
      end
      %{q: q, level: level}
    end)
    |> Map.get(:q)

    q =
      Enum.reduce(colvalops, q, fn %{column: column, operator: operator, value: value}, q ->
        column = String.to_atom(column)
        condition = build_where_condition(column, operator, value)

        where(q, ^condition)
      end)
  end

  def where_nested(q, pathvalop) when is_map(pathvalop) do
    %{operator: operator, value: value, path: path} = pathvalop
    paths = split_by_dots(path)
    {column, paths} = List.pop_at(paths, -1)

    q = paths
      |> List.wrap()
      |> Enum.reduce(q, fn column, q ->
      column = String.to_atom(column)
      join(q, :inner, [log, ..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
    end)

    column = String.to_atom(column)
    condition = build_where_condition(column, operator, value)

    q = where(q, ^condition)
  end

  def group_by_nested_column_path(pathvalops) do
    Enum.group_by(
      pathvalops,
      fn %{path: path} ->
      String.replace(path, ~r/\.\w+$/, "")
    end,
    fn pathvalop ->
      pathvalop
      |> Map.put(:column, String.replace(pathvalop.path, ~r/^[\w\.]+\./, ""))
      |> Map.drop([:path])
    end
    )
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
