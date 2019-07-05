defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query

  def where_nesteds(q, pathvalops) do
    pathvalops
    |> group_by_nested_column_path()
    |> apply_grouped_nested_wheres(q)
  end

  def apply_grouped_nested_wheres(grouped_pathval_opts, q) do
    Enum.reduce(grouped_pathval_opts, q, fn {path, colvalops}, q ->
      where_nested(q, path, colvalops)
    end)
  end

  def where_nested(q, path, colvalops) when is_list(colvalops) do
    if top_level_field?(path) do
      apply_flat_wheres(q, colvalops)
    else
      path
      |> split_by_dots()
      |> apply_nested_joins(q)
      |> apply_flat_wheres(colvalops)
    end
  end

  def top_level_field?(path) do
    path in ~w(timestamp event_message)
  end

  def apply_nested_joins(nested_columns, q) do
    nested_columns
    |> Enum.reduce(%{q: q, level: 0}, fn column, acc ->
      column = String.to_atom(column)
      level = acc.level + 1

      q =
        case level do
          1 ->
            join(acc.q, :inner, [log], n in fragment("UNNEST(?)", field(log, ^column)))

          _ ->
            join(acc.q, :inner, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
        end

      %{q: q, level: level}
    end)
    |> Map.get(:q)
  end

  def apply_flat_wheres(q, colvalops) do
    Enum.reduce(colvalops, q, fn colvalop, q ->
      %{column: column, operator: operator, value: value} = colvalop
      column = String.to_atom(column)
      condition = build_where_condition(column, operator, value)

      where(q, ^condition)
    end)
  end

  def group_by_nested_column_path(pathvalops) do
    Enum.group_by(
      pathvalops,
      fn %{path: path} ->
        # trim last column
        String.replace(path, ~r/\.\w+$/, "")
      end,
      fn pathvalop ->
        pathvalop
        # delete all but last column
        |> Map.put(:column, String.replace(pathvalop.path, ~r/^[\w\.]+\./, ""))
        |> Map.drop([:path])
      end
    )
  end

  def split_by_dots(str) do
    str
    |> String.split(".")
    |> List.wrap()
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
    |> String.replace(~r/\$\d+/, "?")
    # removes double quotes around the names after the dot
    |> String.replace(~r/\."([\w\.]+)"/, ".\\1")
    # removes double quotes around the qualified BQ table id
    |> String.replace(~r/FROM\s+"(.+)"/, "FROM \\1")
  end
end
