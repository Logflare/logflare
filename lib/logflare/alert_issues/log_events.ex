defmodule Logflare.AlertIssues.LogEvents do
  @moduledoc """
  Runs LQL filters against a user's Postgres-backed source.

  The LQL must name its source with `f:<source_name>`. Filter rules compile to SQL
  over the `body` jsonb column with every path and value bound as a parameter.
  Used for alert issue log previews and for scheduled LQL alert rules.
  """

  alias Logflare.AlertIssues.Queries
  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Sources
  alias Logflare.User

  @max_events 100
  @max_range_days 7
  @retention_days 30
  @minute_bucket_max_seconds 3 * 60 * 60
  @top_level ~w(id timestamp event_message)
  @placeholder ~r/\{\{\s*([a-z0-9_]+)\s*\}\}/
  @sample_messages 3

  @type event :: %{String.t() => term()}
  @type bucket :: %{timestamp: DateTime.t(), count: non_neg_integer()}
  @type preview :: %{
          lql: String.t(),
          source: String.t(),
          from: DateTime.t(),
          to: DateTime.t(),
          total: non_neg_integer(),
          buckets: [bucket()],
          events: [event()]
        }

  @spec preview(User.t(), String.t(), keyword()) :: {:ok, preview()} | {:error, term()}
  def preview(%User{} = user, lql, opts) when is_binary(lql) do
    {from, to} = clamp_range(Keyword.get(opts, :from), Keyword.get(opts, :to))

    with {:ok, compiled} <- compile(user, lql),
         {where_sql, params} = range_where(compiled, from, to),
         {:ok, events} <- query(compiled, events_sql(compiled.table, where_sql), params),
         {:ok, buckets} <-
           query(compiled, buckets_sql(compiled.table, where_sql, bucket_unit(from, to)), params) do
      buckets = Enum.map(buckets, &%{timestamp: to_datetime(&1["bucket"]), count: &1["count"]})

      {:ok,
       %{
         lql: lql,
         source: compiled.source.name,
         from: from,
         to: to,
         total: buckets |> Enum.map(& &1.count) |> Enum.sum(),
         buckets: buckets,
         events: Enum.map(events, &to_event/1)
       }}
    end
  end

  @doc """
  Previews the log events behind an alert issue visible within `opts[:filters]`,
  using its rule's `signal_mapping["preview"]["lql"]` (or the query of an LQL rule)
  filled with the alert issue's identifiers.
  """
  @spec for_alert_issue(User.t(), String.t(), keyword()) :: {:ok, preview()} | {:error, term()}
  def for_alert_issue(%User{} = user, alert_issue_id, opts) do
    with {:ok, issue} <- Queries.get(user, alert_issue_id, Keyword.take(opts, [:filters])),
         {:ok, template} <- preview_lql(user, issue["rule_key"]),
         {:ok, lql} <- fill_template(template, issue["identifiers"] || %{}) do
      preview(user, lql, Keyword.take(opts, [:from, :to]))
    end
  end

  defp preview_lql(_user, nil), do: {:error, :no_preview}

  defp preview_lql(user, rule_key) do
    case Alerting.get_alert_query_by(user_id: user.id, rule_key: rule_key) do
      %{signal_mapping: %{"preview" => %{"lql" => lql}}} when is_binary(lql) -> {:ok, lql}
      %{language: :lql, query: lql} -> {:ok, lql}
      _ -> {:error, :no_preview}
    end
  end

  @doc """
  Aggregates matching events since `since`, one row per distinct `group_by` value set.
  Each row has the group paths as keys plus `count`, `first_seen`, `last_seen` and
  `sample_messages`. Returns no rows when nothing matched.
  """
  @spec aggregate(User.t(), String.t(), [String.t()], DateTime.t()) ::
          {:ok, [map()]} | {:error, term()}
  def aggregate(%User{} = user, lql, group_by, %DateTime{} = since) do
    with {:ok, compiled} <- compile(user, lql) do
      {where_sql, params} = range_where(compiled, since, DateTime.utc_now())

      {group_selects, params} =
        group_by
        |> Enum.with_index()
        |> Enum.map_reduce(params, fn {path, i}, acc ->
          {expr, acc} = path_expr(path, acc)
          {"#{expr} as g#{i}", acc}
        end)

      group_cols = Enum.map(Enum.with_index(group_by), fn {_path, i} -> "g#{i}" end)

      sql = """
      select #{Enum.join(group_selects ++ ["count(*) as count", "min(timestamp) as first_seen", "max(timestamp) as last_seen", "(array_agg(event_message order by timestamp desc))[1:#{@sample_messages}] as sample_messages"], ", ")}
      from #{compiled.table}
      where #{where_sql}
      #{if group_cols == [], do: "", else: "group by " <> Enum.join(group_cols, ", ")}
      """

      with {:ok, rows} <- query(compiled, sql, params) do
        {:ok,
         rows
         |> Enum.filter(&(&1["count"] > 0))
         |> Enum.map(&aggregate_row(&1, group_by))}
      end
    end
  end

  defp aggregate_row(row, group_by) do
    group_values =
      group_by
      |> Enum.with_index()
      |> Map.new(fn {path, i} -> {path, row["g#{i}"]} end)

    Map.merge(group_values, %{
      "count" => row["count"],
      "first_seen" => row["first_seen"] |> to_datetime() |> DateTime.to_iso8601(),
      "last_seen" => row["last_seen"] |> to_datetime() |> DateTime.to_iso8601(),
      "sample_messages" => List.wrap(row["sample_messages"])
    })
  end

  @doc """
  Checks that the LQL parses, names a source the user owns on a Postgres backend
  and only uses supported filters.
  """
  @spec validate(User.t(), String.t()) :: :ok | {:error, term()}
  def validate(%User{} = user, lql) when is_binary(lql) do
    with {:ok, _compiled} <- compile(user, lql), do: :ok
  end

  @doc """
  Fills `{{identifier}}` placeholders with quoted identifier values.
  """
  @spec fill_template(String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def fill_template(lql, identifiers) when is_binary(lql) and is_map(identifiers) do
    @placeholder
    |> Regex.scan(lql)
    |> Enum.map(fn [_match, key] -> key end)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, lql}, fn key, {:ok, acc} ->
      case fill_placeholder(acc, key, Map.get(identifiers, key)) do
        {:ok, filled} -> {:cont, {:ok, filled}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp fill_placeholder(_lql, key, nil), do: {:error, {:missing_identifier, key}}

  defp fill_placeholder(lql, key, value) do
    value = to_string(value)

    if String.contains?(value, ["\"", "\\"]) do
      {:error, {:invalid_identifier, key}}
    else
      pattern = Regex.compile!("\\{\\{\\s*#{key}\\s*\\}\\}")
      {:ok, Regex.replace(pattern, lql, ~s("#{value}"))}
    end
  end

  @spec clamp_range(DateTime.t() | nil, DateTime.t() | nil) :: {DateTime.t(), DateTime.t()}
  def clamp_range(from, to) do
    now = DateTime.utc_now()
    to = if is_nil(to) or DateTime.compare(to, now) == :gt, do: now, else: to
    from = from || DateTime.add(to, -24, :hour)

    [from, DateTime.add(to, -@max_range_days, :day), DateTime.add(now, -@retention_days, :day)]
    |> Enum.max(DateTime)
    |> then(&{&1, to})
  end

  defp compile(user, lql) do
    with {:ok, rules} <- parse(lql),
         {:ok, source} <- fetch_source(user, Rules.get_from_rule(rules)),
         {:ok, backend} <- Queries.postgres_backend(user, source),
         {:ok, where_sql, params} <- compile_filters(Rules.get_filter_rules(rules)) do
      {:ok,
       %{
         source: source,
         backend: backend,
         table: PostgresAdaptor.table_name(source),
         where_sql: where_sql,
         params: params
       }}
    end
  end

  defp parse(lql) do
    case Parser.parse(lql) do
      {:ok, rules} -> {:ok, rules}
      {:error, reason} -> {:error, {:invalid_lql, reason}}
    end
  end

  defp fetch_source(_user, nil), do: {:error, :source_required}

  defp fetch_source(user, %FromRule{table: name}) do
    case Sources.Cache.get_by(user_id: user.id, name: name) do
      nil -> {:error, {:unknown_source, name}}
      source -> {:ok, source}
    end
  end

  defp compile_filters(filter_rules) do
    Enum.reduce_while(filter_rules, {:ok, [], []}, fn rule, {:ok, clauses, params} ->
      case filter_clause(rule, params) do
        {:ok, clause, params} -> {:cont, {:ok, clauses ++ [clause], params}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, [], params} -> {:ok, "true", params}
      {:ok, clauses, params} -> {:ok, Enum.join(clauses, " and "), params}
      error -> error
    end
  end

  defp filter_clause(%FilterRule{modifiers: %{negate: true}} = rule, params) do
    with {:ok, clause, params} <-
           filter_clause(%{rule | modifiers: Map.delete(rule.modifiers, :negate)}, params) do
      {:ok, "not coalesce((#{clause}), false)", params}
    end
  end

  defp filter_clause(%FilterRule{operator: :range, values: [low, high], path: path}, params) do
    {expr, params} = path_expr(path, params)
    {low_ref, params} = bind(params, decimal(low))
    {high_ref, params} = bind(params, decimal(high))
    {:ok, "#{numeric(expr, path)} between #{low_ref} and #{high_ref}", params}
  end

  defp filter_clause(%FilterRule{operator: :=, value: :NULL, path: path}, params) do
    {expr, params} = path_expr(path, params)
    {:ok, "#{expr} is null", params}
  end

  defp filter_clause(%FilterRule{operator: op, value: value, path: path}, params)
       when op in [:>, :>=, :<, :<=] do
    {expr, params} = path_expr(path, params)

    if is_number(value) do
      {ref, params} = bind(params, decimal(value))
      {:ok, "#{numeric(expr, path)} #{op} #{ref}", params}
    else
      {ref, params} = bind(params, value)
      {:ok, "#{expr} #{op} #{ref}", params}
    end
  end

  defp filter_clause(%FilterRule{operator: :=, value: value, path: path}, params) do
    {expr, params} = path_expr(path, params)
    {ref, params} = bind(params, to_string(value))
    {:ok, "#{text(expr, path)} = #{ref}", params}
  end

  defp filter_clause(%FilterRule{operator: :"~", value: value, path: path}, params) do
    {expr, params} = path_expr(path, params)
    {ref, params} = bind(params, to_string(value))
    {:ok, "#{text(expr, path)} ~ #{ref}", params}
  end

  defp filter_clause(%FilterRule{operator: :string_contains, value: value, path: path}, params) do
    {expr, params} = path_expr(path, params)
    {ref, params} = bind(params, to_string(value))
    {:ok, "strpos(#{text(expr, path)}, #{ref}) > 0", params}
  end

  defp filter_clause(%FilterRule{operator: :list_includes, value: value, path: path}, params) do
    {path_ref, params} = bind(params, json_path(path))
    {ref, params} = bind(params, Jason.encode!([value]))
    {:ok, "body #> #{path_ref}::text[] @> #{ref}::jsonb", params}
  end

  defp filter_clause(%FilterRule{operator: operator}, _params),
    do: {:error, {:unsupported_operator, operator}}

  defp path_expr(path, params) do
    case normalize_path(path) do
      [column] when column in @top_level ->
        {column, params}

      segments ->
        {ref, params} = bind(params, segments)
        {"(body #>> #{ref}::text[])", params}
    end
  end

  defp normalize_path(path) do
    case String.split(path, ".") do
      ["m" | rest] -> ["metadata" | rest]
      segments -> segments
    end
  end

  defp json_path(path), do: normalize_path(path)

  defp numeric(expr, path) do
    case normalize_path(path) do
      [column] when column in @top_level -> expr
      _ -> "(case when #{expr} ~ '^-?[0-9]+(\\.[0-9]+)?$' then #{expr}::numeric end)"
    end
  end

  defp text(expr, path) do
    case normalize_path(path) do
      ["timestamp"] -> "#{expr}::text"
      _ -> expr
    end
  end

  defp bind(params, value), do: {"$#{length(params) + 1}", params ++ [value]}

  defp decimal(value) when is_integer(value), do: Decimal.new(value)
  defp decimal(value) when is_float(value), do: Decimal.from_float(value)
  defp decimal(value), do: value

  defp range_where(compiled, from, to) do
    {from_ref, params} = bind(compiled.params, from)
    {to_ref, params} = bind(params, to)

    {"timestamp >= #{from_ref} and timestamp < #{to_ref} and (#{compiled.where_sql})", params}
  end

  defp events_sql(table, where_sql) do
    """
    select id::text as id, timestamp, event_message, body::text as body_json
    from #{table}
    where #{where_sql}
    order by timestamp desc
    limit #{@max_events}
    """
  end

  defp buckets_sql(table, where_sql, unit) do
    """
    select date_trunc('#{unit}', timestamp) as bucket, count(*) as count
    from #{table}
    where #{where_sql}
    group by 1
    order by 1
    """
  end

  defp bucket_unit(from, to) do
    if DateTime.diff(to, from) <= @minute_bucket_max_seconds, do: "minute", else: "hour"
  end

  defp query(compiled, sql, params) do
    case PostgresAdaptor.execute_query(compiled.backend, {sql, params}, []) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, _} = error -> error
    end
  end

  defp to_event(%{"body_json" => body_json, "timestamp" => timestamp} = row) do
    row
    |> Map.delete("body_json")
    |> Map.merge(%{"timestamp" => to_datetime(timestamp), "body" => Jason.decode!(body_json)})
  end

  defp to_datetime(value) when is_integer(value), do: DateTime.from_unix!(value, :microsecond)
  defp to_datetime(%DateTime{} = value), do: value
  defp to_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
end
