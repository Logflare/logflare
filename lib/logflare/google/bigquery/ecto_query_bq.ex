defmodule Logflare.EctoQueryBQ do
  @moduledoc false
  import Ecto.Query

  def where_nesteds(q, pathvalops) do
    pathvalops
    |> group_by_nested_column_path()
    |> apply_grouped_nested_wheres(q)
  end

  def where_nested(q, path, colvalops) when is_list(colvalops) do
    if top_level_field?(path) do
      apply_where_conditions(q, colvalops)
    else
      path
      |> split_by_dots()
      |> apply_nested_joins(q)
      |> apply_where_conditions(colvalops)
    end
  end

  def apply_grouped_nested_wheres(grouped_pathval_opts, q) do
    Enum.reduce(grouped_pathval_opts, q, fn {path, colvalops}, q ->
      where_nested(q, path, colvalops)
    end)
  end

  def top_level_field?(path) do
    path in ~w(timestamp event_message)
  end

  def apply_nested_joins(nested_columns, q) do
    nested_columns
    |> Enum.with_index(1)
    |> Enum.reduce(q, fn {column, level}, q ->
      column = String.to_atom(column)

      if level === 1 do
        join(q, :inner, [log], n in fragment("UNNEST(?)", field(log, ^column)))
      else
        join(q, :inner, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
      end
    end)
  end

  def apply_where_conditions(q, colvalops) do
    Enum.reduce(colvalops, q, fn %{column: column, operator: operator, value: value}, q ->
      column = String.to_atom(column)

      where(q, ^build_where_condition(column, operator, value))
    end)
  end

  def group_by_nested_column_path(pathvalops) do
    Enum.group_by(
      pathvalops,
      fn %{path: path} ->
        # delete last column including the dot
        String.replace(path, ~r/\.\w+$/, "")
      end,
      fn pathvalop ->
        pathvalop
        # delete all columns except the last one
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
    op_negated? = is_negated_operator?(op)

    op = if op_negated?, do: String.trim_leading(op, "!"), else: op

    clause =
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
          dynamic([..., n1], fragment(~s|REGEXP_CONTAINS(?, ?)|, field(n1, ^c), ^v))
      end

    if op_negated? do
      dynamic([..., n1], not (^clause))
    else
      clause
    end
  end

  def is_negated_operator?("!" <> _), do: true
  def is_negated_operator?(_), do: false
end
