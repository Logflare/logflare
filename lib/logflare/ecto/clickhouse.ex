defmodule Logflare.Ecto.ClickHouse do
  @moduledoc """
  Converts Ecto queries to ClickHouse SQL.

  Primarily used to go from LQL -> SQL.
  """

  require Logger

  import Logflare.Utils.Guards

  alias Ecto.Query
  alias Ecto.Query.BooleanExpr
  alias Ecto.Query.ByExpr
  alias Ecto.Query.JoinExpr
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.SelectExpr
  alias Ecto.Query.Tagged
  alias Ecto.Query.WithExpr
  alias Ecto.QueryError
  alias Ecto.Queryable
  alias Ecto.SubQuery
  alias __MODULE__.Helpers
  alias __MODULE__.Naming
  alias __MODULE__.Params

  @parent_as __MODULE__

  @binary_ops [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    +: " + ",
    -: " - ",
    *: " * ",
    /: " / ",
    ilike: " ILIKE ",
    like: " LIKE ",
    in: " IN "
  ]

  @binary_op_atoms Keyword.keys(@binary_ops)

  @doc """
  Converts an Ecto query to ClickHouse SQL.

  Accepts either raw Ecto queries or pre-planned queries.

  ## Options

  - `:inline_params` - When `true`, inlines parameters directly into the SQL instead of using
    ClickHouse's `{$N:Type}` parameter syntax. This is used for sandboxed queries.
    Defaults to `false`.
  """
  @spec to_sql(Queryable.t(), opts :: Keyword.t()) ::
          {:ok, {sql :: String.t(), params :: list()}} | {:error, String.t()}
  def to_sql(queryable, opts \\ []) do
    # credo:disable-for-next-line
    try do
      inline_params? = Keyword.get(opts, :inline_params, false)

      query =
        queryable
        |> Queryable.to_query()
        |> ensure_sources()
        |> ensure_select_fields()

      params_acc = %{values: %{}, index_map: %{}, next_ix: 0}
      {query, params_acc} = collect_params(query, params_acc)

      params =
        params_acc.values
        |> Enum.sort_by(fn {ix, _val} -> ix end)
        |> Enum.map(fn {_ix, val} -> val end)

      sql_iodata = all(query, params)
      sql = IO.iodata_to_binary(sql_iodata)

      {sql, params} =
        if inline_params? and not Enum.empty?(params) do
          {inline_parameters_in_sql(sql, params), []}
        else
          {sql, params}
        end

      {:ok, {sql, params}}
    rescue
      e in [QueryError, ArgumentError] ->
        case e do
          %QueryError{message: msg} -> {:error, msg}
          %ArgumentError{} -> {:error, Exception.message(e)}
        end

      e ->
        require Logger

        Logger.error(
          "ClickHouse SQL conversion error: #{Exception.message(e)}\n#{Exception.format_stacktrace(__STACKTRACE__)}"
        )

        {:error, Exception.message(e)}
    end
  end

  defp ensure_sources(%{sources: nil, from: %{source: from_source}, joins: joins} = query)
       when not is_nil(from_source) do
    main_source = normalize_source(from_source)

    {join_sources, joins} =
      joins
      |> Enum.with_index(1)
      |> Enum.map_reduce([], fn {join, ix}, acc ->
        source = normalize_source(join.source)
        updated_join = %{join | ix: ix}
        {source, [updated_join | acc]}
      end)

    joins = Enum.reverse(joins)
    sources = [main_source | join_sources] |> List.to_tuple()
    %{query | sources: sources, joins: joins}
  end

  defp ensure_sources(%{sources: nil} = query) do
    raise QueryError,
      query: query,
      message: "query must have a FROM clause with at least one source"
  end

  defp ensure_sources(query), do: query

  defp normalize_source({:fragment, _, _} = frag), do: frag
  defp normalize_source(%SubQuery{} = sq), do: sq
  defp normalize_source({name, nil}), do: {name, nil, nil}
  defp normalize_source({name, schema, prefix}), do: {name, schema, prefix}
  defp normalize_source({name, schema}), do: {name, schema, nil}

  defp ensure_select_fields(%{select: nil} = query) do
    select = %SelectExpr{
      expr: {:&, [], [0]},
      file: __ENV__.file,
      line: __ENV__.line,
      fields: [{:&, [], [0]}]
    }

    %{query | select: select}
  end

  defp ensure_select_fields(%{select: %{fields: nil} = select} = query) do
    fields = (select.expr && [select.expr]) || [{:&, [], [0]}]
    %{query | select: %{select | fields: fields}}
  end

  defp ensure_select_fields(query), do: query

  defp collect_params(query, acc) do
    {wheres, acc} = collect_from_list(query.wheres, acc)
    {havings, acc} = collect_from_list(query.havings, acc)
    {group_bys, acc} = collect_from_list(query.group_bys, acc)
    {order_bys, acc} = collect_from_list(query.order_bys, acc)
    {joins, acc} = collect_from_list(query.joins, acc)

    {select, acc} =
      if query.select, do: collect_from_expr_item(query.select, acc), else: {query.select, acc}

    {limit_expr, acc} =
      if query.limit, do: collect_from_expr_item(query.limit, acc), else: {query.limit, acc}

    {offset_expr, acc} =
      if query.offset, do: collect_from_expr_item(query.offset, acc), else: {query.offset, acc}

    acc =
      case query.with_ctes do
        %WithExpr{queries: queries} when is_list(queries) ->
          Enum.reduce(queries, acc, fn {_name, _opts, cte_query}, acc ->
            # credo:disable-for-next-line
            case cte_query do
              %Query{} = q ->
                {_q, acc} = collect_params(q, acc)
                acc

              _other ->
                acc
            end
          end)

        _ ->
          acc
      end

    query = %{
      query
      | wheres: wheres,
        havings: havings,
        group_bys: group_bys,
        order_bys: order_bys,
        joins: joins,
        select: select,
        limit: limit_expr,
        offset: offset_expr
    }

    query =
      case Map.get(acc, :subqueries) do
        nil -> query
        subqueries_map -> Map.put(query, :__subqueries__, subqueries_map)
      end

    {query, acc}
  end

  defp collect_from_list(list, acc) when is_list(list) do
    Enum.map_reduce(list, acc, &collect_from_expr_item/2)
  end

  defp collect_from_expr_item(%{expr: expr, params: params, subqueries: subqueries} = item, acc)
       when is_list(params) and is_list(subqueries) do
    real_params =
      Enum.reject(params, fn
        {key, _ix} when is_atom(key) and key == :subquery -> true
        _ -> false
      end)

    {expr, acc} = process_params_and_expr(expr, real_params, acc)
    acc = collect_subqueries(subqueries, acc)
    {expr, acc} = collect_from_expr(expr, acc)
    {%{item | expr: expr}, acc}
  end

  defp collect_from_expr_item(%{expr: expr, params: params} = item, acc) when is_list(params) do
    {expr, acc} = process_params_and_expr(expr, params, acc)
    {expr, acc} = collect_from_expr(expr, acc)
    {%{item | expr: expr}, acc}
  end

  defp collect_from_expr_item(%{expr: expr, subqueries: subqueries} = item, acc)
       when is_list(subqueries) do
    acc = collect_subqueries(subqueries, acc)
    {expr, acc} = collect_from_expr(expr, acc)
    {%{item | expr: expr}, acc}
  end

  defp collect_from_expr_item(%{expr: expr} = item, acc) do
    {expr, acc} = collect_from_expr(expr, acc)
    {%{item | expr: expr}, acc}
  end

  defp collect_from_expr_item(item, acc), do: {item, acc}

  defp process_params_and_expr(expr, params, acc) do
    {acc, param_transform, total_offset} =
      params
      |> Enum.with_index()
      |> Enum.reduce({acc, %{}, 0}, fn {{value, type_info}, param_idx},
                                       {acc, transforms, offset} ->
        process_single_param(value, type_info, param_idx, acc, transforms, offset)
      end)

    acc = %{acc | next_ix: acc.next_ix + total_offset}
    expr = transform_in_params(expr, param_transform)
    {expr, acc}
  end

  defp process_single_param(list_value, {:in, _field_info}, param_idx, acc, transforms, offset)
       when is_list(list_value) do
    start_ix = acc.next_ix + offset
    list_length = length(list_value)

    acc =
      Enum.reduce(Enum.with_index(list_value), acc, fn {elem, elem_offset}, acc ->
        param_ix = start_ix + elem_offset
        %{acc | values: Map.put(acc.values, param_ix, elem)}
      end)

    transforms = Map.put(transforms, param_idx, {start_ix, list_length})
    {acc, transforms, offset + list_length}
  end

  defp process_single_param(value, _type_or_field, param_idx, acc, transforms, offset) do
    param_ix = acc.next_ix + offset
    acc = %{acc | values: Map.put(acc.values, param_ix, value)}
    transforms = Map.put(transforms, param_idx, param_ix)
    {acc, transforms, offset + 1}
  end

  defp transform_in_params({:^, meta, [ix]}, transforms) do
    case Map.get(transforms, ix) do
      new_ix when is_integer(new_ix) -> {:^, meta, [new_ix]}
      {new_ix, _len} -> {:^, meta, [new_ix]}
      nil -> {:^, meta, [ix]}
    end
  end

  defp transform_in_params({:in, meta, [left, {:^, param_meta, [ix]}]}, transforms) do
    case Map.get(transforms, ix) do
      {new_ix, len} -> {:in, meta, [left, {:^, param_meta, [new_ix, len]}]}
      nil -> {:in, meta, [left, {:^, param_meta, [ix]}]}
    end
  end

  defp transform_in_params(tuple, transforms) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.map(&transform_in_params(&1, transforms))
    |> List.to_tuple()
  end

  defp transform_in_params(list, transforms) when is_list(list) do
    Enum.map(list, &transform_in_params(&1, transforms))
  end

  defp transform_in_params(other, _transforms), do: other

  defp collect_subqueries(subqueries, acc) do
    subqueries_map = acc[:subqueries] || %{}

    {subqueries_map, acc} =
      subqueries
      |> Enum.with_index()
      |> Enum.reduce({subqueries_map, acc}, fn {%SubQuery{query: subquery_query} = subquery, ix},
                                               {map, acc} ->
        {_subquery_query, acc} = collect_params(subquery_query, acc)
        {Map.put(map, ix, subquery), acc}
      end)

    Map.put(acc, :subqueries, subqueries_map)
  end

  defp collect_from_expr(%Tagged{value: values}, acc) when is_list(values) do
    {param_refs, acc} =
      Enum.map_reduce(values, acc, fn value, acc ->
        case Map.get(acc.index_map, value) do
          nil ->
            ix = acc.next_ix
            param_ref = {:^, [], [ix]}

            acc = %{
              acc
              | values: Map.put(acc.values, ix, value),
                index_map: Map.put(acc.index_map, value, ix),
                next_ix: ix + 1
            }

            {param_ref, acc}

          ix ->
            {{:^, [], [ix]}, acc}
        end
      end)

    {param_refs, acc}
  end

  defp collect_from_expr(%Tagged{value: value}, acc) do
    case Map.get(acc.index_map, value) do
      nil ->
        ix = acc.next_ix
        param_ref = {:^, [], [ix]}

        acc = %{
          acc
          | values: Map.put(acc.values, ix, value),
            index_map: Map.put(acc.index_map, value, ix),
            next_ix: ix + 1
        }

        {param_ref, acc}

      ix ->
        {{:^, [], [ix]}, acc}
    end
  end

  defp collect_from_expr(tuple, acc) when is_tuple(tuple) do
    {list, acc} =
      tuple
      |> Tuple.to_list()
      |> Enum.map_reduce(acc, &collect_from_expr/2)

    {List.to_tuple(list), acc}
  end

  defp collect_from_expr(list, acc) when is_list(list) do
    Enum.map_reduce(list, acc, &collect_from_expr/2)
  end

  defp collect_from_expr(other, acc), do: {other, acc}

  defp all(query, params, as_prefix \\ [])

  defp all(%{lock: lock}, _params, _as_prefix) when not is_nil(lock) do
    raise ArgumentError, "ClickHouse does not support locks"
  end

  defp all(query, params, as_prefix) do
    sources = Naming.create_names(query.sources, as_prefix)

    [
      cte(query, sources, params),
      select(query, sources, params),
      from(query, sources, params),
      join(query, sources, params),
      where(query, sources, params),
      group_by(query, sources, params),
      having(query, sources, params),
      window(query, sources, params),
      order_by(query, sources, params),
      limit(query, sources, params),
      offset(query, sources, params),
      combinations(query, params)
    ]
  end

  defp select(%{select: %{fields: fields}, distinct: distinct} = query, sources, params) do
    [
      "SELECT ",
      distinct(distinct, sources, params, query)
      | select_fields(fields, sources, params, query)
    ]
  end

  defp select_fields([], _sources, _params, _query), do: "true"

  defp select_fields(fields, sources, params, query) do
    Helpers.intersperse_map(fields, ?,, fn
      {:&, _, [idx]} ->
        {_, source, _} = elem(sources, idx)
        source

      {:%{}, _, kv_pairs} ->
        Helpers.intersperse_map(kv_pairs, ?,, fn {k, v} ->
          [expr(v, sources, params, query), " AS " | Naming.quote_name(k)]
        end)

      {:{}, _, exprs} ->
        Helpers.intersperse_map(exprs, ?,, &expr(&1, sources, params, query))

      {k, v} ->
        [expr(v, sources, params, query), " AS " | Naming.quote_name(k)]

      v ->
        expr(v, sources, params, query)
    end)
  end

  defp distinct(nil, _sources, _params, _query), do: []
  defp distinct(%{expr: true}, _sources, _params, _query), do: "DISTINCT "
  defp distinct(%{expr: false}, _sources, _params, _query), do: []

  defp distinct(%{expr: exprs}, sources, params, query) when is_list(exprs) do
    [
      "DISTINCT ON (",
      Helpers.intersperse_map(exprs, ?,, &order_by_expr(&1, sources, params, query)),
      ") "
    ]
  end

  defp from(%{from: %{source: source, hints: hints}} = query, sources, params) do
    {from, name} = get_source(query, sources, params, 0, source)
    [" FROM ", from, " AS ", name | hints(hints)]
  end

  defp cte(
         %{with_ctes: %WithExpr{recursive: recursive, queries: [_ | _] = queries}} = query,
         sources,
         params
       ) do
    recursive_opt = if recursive, do: "RECURSIVE ", else: ""

    ctes =
      Helpers.intersperse_map(queries, ?,, fn {name, _opts, cte} ->
        [Naming.quote_name(name), " AS ", cte_query(cte, sources, params, query)]
      end)

    ["WITH ", recursive_opt, ctes, " "]
  end

  defp cte(%{with_ctes: _}, _sources, _params), do: []

  defp cte_query(%Query{} = query, sources, params, parent_query) do
    query = ensure_sources(query)
    query = ensure_select_fields(query)
    query = put_in(query.aliases[@parent_as], {parent_query, sources})
    [?(, all(query, params, Naming.subquery_as_prefix(sources)), ?)]
  end

  defp cte_query(%QueryExpr{expr: expr}, sources, params, query) do
    expr(expr, sources, params, query)
  end

  defp join(%{joins: []}, _sources, _params), do: []

  defp join(%{joins: joins} = query, sources, params) do
    Enum.map(joins, fn
      %JoinExpr{qual: qual, ix: ix, source: source, on: %QueryExpr{expr: on_exrp}, hints: hints} ->
        {join, name} = get_source(query, sources, params, ix, source)

        [
          join_hints(hints, query),
          join_qual(qual, hints),
          join,
          " AS ",
          name
          | join_on(qual, on_exrp, hints, sources, params, query)
        ]
    end)
  end

  valid_join_strictness_hints = ["ASOF", "ANY", "ANTI", "SEMI"]
  valid_join_hints = valid_join_strictness_hints ++ ["ARRAY"]

  for hint <- valid_join_strictness_hints do
    hints = List.wrap(hint)

    defp join_hints(unquote(hints), _query) do
      unquote(" " <> Enum.join(hints, " "))
    end
  end

  defp join_hints(["ARRAY"], _query), do: []
  defp join_hints([], _query), do: []

  defp join_hints(hints, query) do
    supported = unquote(valid_join_hints) |> Enum.map(&inspect/1) |> Enum.join(", ")

    raise QueryError,
      query: query,
      message: """
      unsupported JOIN strictness or type passed in hints: #{inspect(hints)}
      supported: #{supported}
      """
  end

  defp join_on(:cross, true, _hints, _sources, _params, _query), do: []

  defp join_on(_qual, true, ["ARRAY"], _sources, _params, _query) do
    []
  end

  defp join_on(_qual, expr, _hints, sources, params, query) do
    [" ON " | expr(expr, sources, params, query)]
  end

  defp join_qual(:inner, ["ARRAY"]), do: " ARRAY JOIN "
  defp join_qual(:inner, _hints), do: " INNER JOIN "
  defp join_qual(:left, ["ARRAY"]), do: " LEFT ARRAY JOIN "
  defp join_qual(:left, _hints), do: " LEFT JOIN "
  defp join_qual(:right, _hints), do: " RIGHT JOIN "
  defp join_qual(:full, _hints), do: " FULL JOIN "
  defp join_qual(:cross, _hints), do: " CROSS JOIN "

  defp join_qual(qual, _hints) do
    raise ArgumentError, "join type #{inspect(qual)} is not supported"
  end

  defp where(%{wheres: wheres} = query, sources, params) do
    boolean(" WHERE ", wheres, sources, params, query)
  end

  defp having(%{havings: havings} = query, sources, params) do
    boolean(" HAVING ", havings, sources, params, query)
  end

  defp group_by(%{group_bys: []}, _sources, _params), do: []

  defp group_by(%{group_bys: group_bys} = query, sources, params) do
    [
      " GROUP BY "
      | Helpers.intersperse_map(group_bys, ?,, fn %ByExpr{expr: expr} ->
          Helpers.intersperse_map(expr, ?,, &expr(&1, sources, params, query))
        end)
    ]
  end

  defp window(%{windows: []}, _sources, _params), do: []

  defp window(%{windows: windows} = query, sources, params) do
    [
      " WINDOW "
      | Helpers.intersperse_map(windows, ?,, fn {name, %{expr: kw}} ->
          [Naming.quote_name(name), " AS " | window_exprs(kw, sources, params, query)]
        end)
    ]
  end

  defp window_exprs(kw, sources, params, query) do
    [
      ?(,
      Helpers.intersperse_map(kw, ?\s, &window_expr(&1, sources, params, query)),
      ?)
    ]
  end

  defp window_expr({:partition_by, fields}, sources, params, query) do
    ["PARTITION BY " | Helpers.intersperse_map(fields, ?,, &expr(&1, sources, params, query))]
  end

  defp window_expr({:order_by, fields}, sources, params, query) do
    [
      "ORDER BY "
      | Helpers.intersperse_map(fields, ?,, &order_by_expr(&1, sources, params, query))
    ]
  end

  defp window_expr({:frame, {:fragment, _, _} = fragment}, sources, params, query) do
    expr(fragment, sources, params, query)
  end

  defp order_by(%{order_bys: []}, _sources, _params), do: []

  defp order_by(%{order_bys: order_bys} = query, sources, params) do
    [
      " ORDER BY "
      | Helpers.intersperse_map(order_bys, ?,, fn %{expr: expr} ->
          Helpers.intersperse_map(expr, ?,, &order_by_expr(&1, sources, params, query))
        end)
    ]
  end

  defp order_by_expr({:asc, expr}, sources, params, query) do
    expr(expr, sources, params, query)
  end

  defp order_by_expr({:desc, expr}, sources, params, query) do
    [expr(expr, sources, params, query), " DESC"]
  end

  defp order_by_expr({:asc_nulls_first, expr}, sources, params, query) do
    [expr(expr, sources, params, query), " ASC NULLS FIRST"]
  end

  defp order_by_expr({:desc_nulls_first, expr}, sources, params, query) do
    [expr(expr, sources, params, query), " DESC NULLS FIRST"]
  end

  defp order_by_expr({:asc_nulls_last, expr}, sources, params, query) do
    [expr(expr, sources, params, query), " ASC NULLS LAST"]
  end

  defp order_by_expr({:desc_nulls_last, expr}, sources, params, query) do
    [expr(expr, sources, params, query), " DESC NULLS LAST"]
  end

  defp order_by_expr({dir, _expr}, _sources, _params, query) do
    raise QueryError,
      query: query,
      message: "ClickHouse does not support #{dir} in ORDER BY"
  end

  defp limit(%{limit: nil}, _sources, _params), do: []

  defp limit(%{limit: %{expr: expr}} = query, sources, params) do
    [" LIMIT ", expr(expr, sources, params, query)]
  end

  defp offset(%{offset: nil}, _sources, _params), do: []

  defp offset(%{offset: %{expr: expr}} = query, sources, params) do
    [" OFFSET ", expr(expr, sources, params, query)]
  end

  defp combinations(%{combinations: combinations}, params) do
    Enum.map(combinations, &combination(&1, params))
  end

  defp combination({:union, query}, params), do: [" UNION DISTINCT (", all(query, params), ?)]
  defp combination({:union_all, query}, params), do: [" UNION ALL (", all(query, params), ?)]
  defp combination({:except, query}, params), do: [" EXCEPT (", all(query, params), ?)]
  defp combination({:intersect, query}, params), do: [" INTERSECT (", all(query, params), ?)]

  defp combination({:except_all, query}, _params) do
    raise QueryError,
      query: query,
      message: "ClickHouse does not support EXCEPT ALL"
  end

  defp combination({:intersect_all, query}, _params) do
    raise QueryError,
      query: query,
      message: "ClickHouse does not support INTERSECT ALL"
  end

  defp hints([_ | _] = hints) do
    [" " | Helpers.intersperse_map(hints, ?\s, &hint/1)]
  end

  defp hints([]), do: []

  defp hint(hint) when is_binary(hint), do: hint

  defp hint({k, v}) when is_atom(k) and is_integer(v) do
    [Atom.to_string(k), ?\s, Integer.to_string(v)]
  end

  defp boolean(_name, [], _sources, _params, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | exprs], sources, params, query) do
    result =
      Enum.reduce(exprs, {op, paren_expr(expr, sources, params, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op) | paren_expr(expr, sources, params, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op) | paren_expr(expr, sources, params, query)]}
      end)

    [name | elem(result, 1)]
  end

  defp operator_to_boolean(:and), do: " AND "
  defp operator_to_boolean(:or), do: " OR "

  defp parens_for_select([first_expr | _] = expression) do
    if is_binary(first_expr) and String.match?(first_expr, ~r/^\s*select/i) do
      [?(, expression, ?)]
    else
      expression
    end
  end

  defp paren_expr(expr, sources, params, query) do
    [?(, expr(expr, sources, params, query), ?)]
  end

  defp expr({_type, [literal]}, sources, params, query) do
    expr(literal, sources, params, query)
  end

  defp expr({:^, [], [ix]}, _sources, params, _query) do
    Params.build_param(ix, Enum.at(params, ix))
  end

  defp expr({:^, [], [ix, len]}, _sources, params, _query) when len > 0 do
    [?(, Params.build_params(ix, len, params), ?)]
  end

  defp expr({:^, [], [_, 0]}, _sources, _params, _query), do: "[]"

  defp expr({{:., _, [{:&, _, [ix]}, field]}, _, []}, sources, _params, _query)
       when is_atom(field) or is_binary(field) do
    Naming.field_access(field, sources, ix)
  end

  defp expr({{:., _, [{:parent_as, _, [as]}, field]}, _, []}, _sources, _params, query)
       when is_atom(field) or is_binary(field) do
    {ix, sources} = get_parent_sources_ix(query, as)
    Naming.field_access(field, sources, ix)
  end

  defp expr({:&, _, [ix]}, sources, _params, _query) do
    {_, source, _} = elem(sources, ix)
    source
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, _params, query) do
    {_, name, schema} = elem(sources, idx)

    if is_nil(schema) and is_nil(fields) do
      raise QueryError,
        query: query,
        message:
          "ClickHouse requires a schema module when using selector " <>
            "#{inspect(name)} but none was given. " <>
            "Please specify a schema or specify exactly which fields from " <>
            "#{inspect(name)} you desire"
    end

    Helpers.intersperse_map(fields, ?,, &[name, ?. | Naming.quote_name(&1)])
  end

  defp expr({:in, _, [_left, []]}, _sources, _params, _query), do: "0"
  defp expr({:in, _, [_, {:^, _, [_ix, 0]}]}, _sources, _params, _query), do: "0"

  defp expr({:in, _, [left, right]}, sources, params, query) when is_list(right) do
    args = Helpers.intersperse_map(right, ?,, &expr(&1, sources, params, query))
    [expr(left, sources, params, query), " IN (", args, ?)]
  end

  defp expr({:in, _, [left, right]}, sources, params, query) do
    [expr(left, sources, params, query), " IN ", expr(right, sources, params, query)]
  end

  defp expr({:is_nil, _, [arg]}, sources, params, query) do
    ["isNull(", expr(arg, sources, params, query), ?)]
  end

  defp expr({:not, _, [{:is_nil, _, [arg]}]}, sources, params, query) do
    ["isNotNull(", expr(arg, sources, params, query), ?)]
  end

  defp expr({:not, _, [{:like, _, [l, r]}]}, sources, params, query) do
    ["notLike(", expr(l, sources, params, query), ", ", expr(r, sources, params, query), ?)]
  end

  defp expr({:not, _, [{:ilike, _, [l, r]}]}, sources, params, query) do
    ["notILike(", expr(l, sources, params, query), ", ", expr(r, sources, params, query), ?)]
  end

  defp expr({:not, _, [inner_expr]}, sources, params, query) do
    ["not(", expr(inner_expr, sources, params, query), ?)]
  end

  defp expr({:filter, _, [agg, filter]}, sources, params, query) do
    [
      expr(agg, sources, params, query),
      " FILTER (WHERE ",
      expr(filter, sources, params, query),
      ?)
    ]
  end

  defp expr(%SubQuery{query: query}, sources, params, parent_query) do
    query = ensure_sources(query)
    query = ensure_select_fields(query)
    query = put_in(query.aliases[@parent_as], {parent_query, sources})
    [?(, all(query, params, Naming.subquery_as_prefix(sources)), ?)]
  end

  defp expr({:subquery, ix}, sources, params, query) do
    case query do
      %{__subqueries__: subqueries} when is_map(subqueries) ->
        case Map.get(subqueries, ix) do
          nil ->
            raise QueryError,
              query: query,
              message: "subquery at index #{ix} not found in subqueries map"

          subquery ->
            expr(subquery, sources, params, query)
        end

      _ ->
        raise QueryError,
          query: query,
          message:
            "subquery at index #{ix} - subqueries must be pre-evaluated before SQL conversion"
    end
  end

  defp expr({:fragment, _, [kw]}, _sources, _params, query)
       when is_list(kw) or tuple_size(kw) == 3 do
    raise QueryError,
      query: query,
      message: "ClickHouse does not currently support keyword or interpolated fragments"
  end

  defp expr({:fragment, _, parts}, sources, params, query) do
    parts
    |> Enum.map(fn
      {:raw, part} ->
        part

      {:expr, expr} ->
        expr(expr, sources, params, query)
    end)
    |> parens_for_select()
  end

  defp expr({:literal, _, [literal]}, _sources, _params, _query) do
    Naming.quote_name(literal)
  end

  defp expr({:values, _, [types, idx, num_rows]}, _sources, params, query) do
    rows = :lists.seq(1, num_rows, 1)

    structure =
      Enum.map_intersperse(types, ?,, fn {field, type} ->
        [Naming.escape_string(Atom.to_string(field)), ?\s, Helpers.ecto_to_db(type, query)]
      end)

    {rows, _idx} =
      Helpers.intersperse_reduce(rows, ?,, idx, fn _, idx ->
        {value, idx} = values_expr(types, idx, params)
        {[?(, value, ?)], idx}
      end)

    ["VALUES('", structure, ?', ?,, rows, ?)]
  end

  defp expr({:identifier, _, [name]}, _sources, _params, _query) do
    Naming.quote_name(name)
  end

  defp expr({:constant, _, [literal]}, _sources, _params, _query) when is_binary(literal) do
    [?', Naming.escape_string(literal), ?']
  end

  defp expr({:constant, _, [literal]}, _sources, _params, _query) when is_number(literal) do
    [to_string(literal)]
  end

  defp expr({:splice, _, [{:^, _, [idx, length]}]}, _sources, params, _query) do
    Enum.map_join(1..length, ",", fn i ->
      pos = idx + i - 1
      Params.build_param(pos, Enum.at(params, pos))
    end)
  end

  defp expr({:selected_as, _, [name]}, _sources, _params, _query) do
    Naming.quote_name(name)
  end

  defp expr({:over, _, [agg, name]}, sources, params, query) when is_atom_value(name) do
    [expr(agg, sources, params, query), " OVER " | Naming.quote_name(name)]
  end

  defp expr({:over, _, [agg, kw]}, sources, params, query) do
    [expr(agg, sources, params, query), " OVER " | window_exprs(kw, sources, params, query)]
  end

  defp expr({:{}, _, elems}, sources, params, query) do
    [?(, Helpers.intersperse_map(elems, ?,, &expr(&1, sources, params, query)), ?)]
  end

  defp expr({:count, _, []}, _sources, _params, _query), do: "count(*)"

  defp expr({:count, _, [expr]}, sources, params, query) do
    ["count(", expr(expr, sources, params, query), ?)]
  end

  defp expr({:count, _, [expr, :distinct]}, sources, params, query) do
    ["countDistinct(", expr(expr, sources, params, query), ?)]
  end

  defp expr({:datetime_add, _, [datetime, count, interval]}, sources, params, query) do
    [
      expr(datetime, sources, params, query),
      " + ",
      Helpers.interval(count, interval, sources, params, query)
    ]
  end

  defp expr({:date_add, _, [date, count, interval]}, sources, params, query) do
    [
      "CAST(",
      expr(date, sources, params, query),
      " + ",
      Helpers.interval(count, interval, sources, params, query),
      " AS Date)"
    ]
  end

  defp expr({:json_extract_path, _, [expr, path]}, sources, params, query) do
    path =
      Enum.map(path, fn
        bin when is_binary(bin) -> [?., Naming.escape_json_key(bin)]
        int when is_integer(int) -> [?[, Integer.to_string(int), ?]]
      end)

    ["JSON_QUERY(", expr(expr, sources, params, query), ", '$", path | "')"]
  end

  defp expr({:exists, _, [subquery]}, sources, params, query) do
    ["exists" | expr(subquery, sources, params, query)]
  end

  defp expr({op, _, [l, r]}, sources, params, query) when op in [:and, :or] do
    [
      logical_expr(op, l, sources, params, query),
      operator_to_boolean(op),
      logical_expr(op, r, sources, params, query)
    ]
  end

  defp expr({fun, _, args}, sources, params, query) when is_atom(fun) and is_list(args) do
    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args

        [
          maybe_paren_expr(left, sources, params, query),
          op | maybe_paren_expr(right, sources, params, query)
        ]

      {:fun, fun} ->
        [fun, ?(, Helpers.intersperse_map(args, ?,, &expr(&1, sources, params, query)), ?)]
    end
  end

  defp expr(list, sources, params, query) when is_list(list) do
    [?[, Helpers.intersperse_map(list, ?,, &expr(&1, sources, params, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _params, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Tagged{value: value, type: :any}, sources, params, query) do
    expr(value, sources, params, query)
  end

  defp expr(%Tagged{value: value, type: type}, sources, params, query) do
    ["CAST(", expr(value, sources, params, query), " AS ", Helpers.ecto_to_db(type, query), ?)]
  end

  defp expr(nil, _sources, _params, _query), do: "NULL"
  defp expr(true, _sources, _params, _query), do: "1"
  defp expr(false, _sources, _params, _query), do: "0"

  defp expr(literal, _sources, _params, _query) when is_binary(literal) do
    [?', Naming.escape_string(literal), ?']
  end

  defp expr(literal, _sources, _params, _query) when is_integer(literal) do
    Params.inline_param(literal)
  end

  defp expr(literal, _sources, _params, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp expr(expr, _sources, _params, query) do
    raise QueryError,
      query: query,
      message: "unsupported expression #{inspect(expr)}"
  end

  defp logical_expr(parent_op, expr, sources, params, query) do
    case expr do
      {^parent_op, _, [l, r]} ->
        [
          logical_expr(parent_op, l, sources, params, query),
          operator_to_boolean(parent_op),
          logical_expr(parent_op, r, sources, params, query)
        ]

      {op, _, [l, r]} when op in [:and, :or] ->
        [
          ?(,
          logical_expr(op, l, sources, params, query),
          operator_to_boolean(op),
          logical_expr(op, r, sources, params, query),
          ?)
        ]

      _ ->
        maybe_paren_expr(expr, sources, params, query)
    end
  end

  defp maybe_paren_expr({op, _, [_, _]} = expr, sources, params, query)
       when op in @binary_op_atoms do
    paren_expr(expr, sources, params, query)
  end

  defp maybe_paren_expr(expr, sources, params, query) do
    expr(expr, sources, params, query)
  end

  defp values_expr(types, idx, params) do
    Helpers.intersperse_reduce(types, ?,, idx, fn {_field, _type}, idx ->
      {Params.build_param(idx, Enum.at(params, idx)), idx + 1}
    end)
  end

  for {op, str} <- @binary_ops do
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp get_source(query, sources, params, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || expr(source, sources, params, query), name}
  end

  defp get_parent_sources_ix(query, as) do
    case query.aliases[@parent_as] do
      {%{aliases: %{^as => ix}}, sources} -> {ix, sources}
      {%{} = parent, _sources} -> get_parent_sources_ix(parent, as)
    end
  end

  @spec inline_parameters_in_sql(sql :: String.t(), params :: list()) :: String.t()
  defp inline_parameters_in_sql(sql, params) when is_binary(sql) and is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.reduce(sql, fn {param_value, index}, acc_sql ->
      # Match {$N:Type} pattern (case-insensitive for type)
      pattern = ~r/\{\$#{index}:[^}]+\}/i
      replacement = Params.inline_param(param_value) |> IO.iodata_to_binary()
      String.replace(acc_sql, pattern, replacement, global: false)
    end)
  end
end
