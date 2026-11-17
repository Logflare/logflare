defmodule Logflare.AlertIssues.Queries do
  @moduledoc """
  Code-defined read queries over `system.alert_issues`.

  The SQL lives here rather than in user-visible endpoints. Queries are scoped by
  generic identifier filters (`filters[identifiers.<key>]=<value>`): keys must match
  `#{inspect(~r/^[a-z0-9_]{1,64}$/)}` and keys and values are bound as parameters.
  """

  alias Logflare.AlertIssues.Silences
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.QueryError
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users

  @retention_days 30
  @max_limit 500
  @operator_limit 50
  @filter_key ~r/^[a-z0-9_]{1,64}$/

  @type opt ::
          {:filters, %{optional(String.t()) => String.t()}}
          | {:statuses, [String.t()]}
          | {:since, DateTime.t()}
          | {:limit, pos_integer()}
          | {:reader, String.t()}
          | {:include_silenced, boolean()}

  @spec list(User.t(), [opt()]) :: {:ok, [map()]} | {:error, term()}
  def list(%User{} = user, opts) do
    with {:ok, rows} <- run(user, opts, nil) do
      statuses = Keyword.get(opts, :statuses)

      rows =
        rows
        |> Enum.filter(&(is_nil(statuses) or &1["status"] in statuses))
        |> put_silenced(Silences.list(user), Keyword.get(opts, :include_silenced, false))

      put_unread(user, rows, Keyword.get(opts, :reader))
    end
  end

  defp put_silenced(rows, silences, include_silenced?) do
    rows
    |> Enum.map(
      &Map.put(&1, "silenced", Silences.silenced?(silences, &1["rule_key"], &1["identifiers"]))
    )
    |> Enum.filter(&(include_silenced? or not &1["silenced"]))
  end

  defp put_unread(_user, rows, nil), do: {:ok, rows}
  defp put_unread(_user, [], _reader), do: {:ok, []}

  defp put_unread(user, rows, reader) do
    with {:ok, reads} <- latest_reads(user, reader, Enum.map(rows, & &1["alert_issue_id"])) do
      {:ok,
       Enum.map(rows, &Map.put(&1, "unread", unread?(&1, Map.get(reads, &1["alert_issue_id"]))))}
    end
  end

  defp unread?(_issue, nil), do: true
  defp unread?(_issue, %{"read" => false}), do: true

  defp unread?(%{"attention_at" => attention_at}, %{"read" => true, "version" => version})
       when is_binary(attention_at) do
    {:ok, attention, _offset} = DateTime.from_iso8601(attention_at)
    version < DateTime.to_unix(attention, :microsecond)
  end

  defp unread?(_issue, %{"read" => true}), do: false

  defp latest_reads(user, reader, alert_issue_ids) do
    with %_{} = source <-
           Sources.Cache.get_by(user_id: user.id, system_source_type: :alert_issue_reads) ||
             {:error, :no_alert_issues},
         {:ok, backend} <- postgres_backend(user, source) do
      sql = """
      select distinct on (body ->> 'alert_issue_id') body::text as body_json
      from #{PostgresAdaptor.table_name(source)}
      where body ->> 'reader' = $1
        and body ->> 'alert_issue_id' = any($2)
        and timestamp >= $3
      order by body ->> 'alert_issue_id', cast(body ->> 'version' as bigint) desc
      """

      backend
      |> PostgresAdaptor.execute_query({sql, [reader, alert_issue_ids, retention_floor()]}, [])
      |> decode_bodies(%{}, &Map.new(&1, fn read -> {read["alert_issue_id"], read} end))
    else
      {:error, :no_alert_issues} -> {:ok, %{}}
      {:error, _} = error -> error
    end
  end

  @doc """
  Lists the latest version of each comment on an alert issue visible within
  `opts[:filters]`, oldest first.
  """
  @spec comments(User.t(), String.t(), [opt()]) :: {:ok, [map()]} | {:error, term()}
  def comments(%User{} = user, alert_issue_id, opts) when is_binary(alert_issue_id) do
    with {:ok, _issue} <- get(user, alert_issue_id, Keyword.take(opts, [:filters])) do
      sql = """
      select body_json from (
        select distinct on (body ->> 'comment_id') body::text as body_json, body ->> 'created_at' as created_at
        from %{table}
        where body ->> 'alert_issue_id' = $1
          and timestamp >= $2
        order by body ->> 'comment_id', cast(body ->> 'version' as bigint) desc
      ) latest
      order by created_at
      """

      system_query(user, :alert_issue_comments, sql, [alert_issue_id, retention_floor()], [])
    end
  end

  defp system_query(user, type, sql, params, empty) do
    with %_{} = source <-
           Sources.Cache.get_by(user_id: user.id, system_source_type: type) ||
             {:error, :no_source},
         {:ok, backend} <- postgres_backend(user, source) do
      sql = String.replace(sql, "%{table}", PostgresAdaptor.table_name(source))

      backend
      |> PostgresAdaptor.execute_query({sql, params}, [])
      |> decode_bodies(empty, &Function.identity/1)
    else
      {:error, :no_source} -> {:ok, empty}
      {:error, _} = error -> error
    end
  end

  @spec get(User.t(), String.t(), [opt()]) :: {:ok, map()} | {:error, term()}
  def get(%User{} = user, alert_issue_id, opts) when is_binary(alert_issue_id) do
    case run(user, opts, alert_issue_id) do
      {:ok, [issue | _]} -> {:ok, issue}
      {:ok, []} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  @doc """
  Internal lookup: the newest stored version of the alert issue for a user and
  fingerprint, used when the node-local active cache has no entry for it.
  """
  @spec latest_by_fingerprint(pos_integer(), String.t()) :: {:ok, map() | nil} | {:error, term()}
  def latest_by_fingerprint(user_id, fingerprint) when is_binary(fingerprint) do
    with %User{} = user <- Users.Cache.get(user_id),
         {:ok, rows} <- latest(user, [], [{"fingerprint", fingerprint}], nil, 1) do
      {:ok, List.first(rows)}
    else
      nil -> {:ok, nil}
      {:error, :unsupported_backend} -> {:ok, nil}
      {:error, _} = error -> error
    end
  end

  @doc """
  Operator view: the latest version of each alert issue for one rule, across all
  identifiers, with the `silenced` flag. Newest first, at most #{@operator_limit}.
  """
  @spec list_for_rule(User.t(), String.t()) :: {:ok, [map()]} | {:error, term()}
  def list_for_rule(%User{} = user, rule_key) when is_binary(rule_key) do
    with {:ok, rows} <- latest(user, [], [{"rule_key", rule_key}], nil, @operator_limit) do
      {:ok, put_silenced(rows, Silences.list(user), true)}
    end
  end

  @doc """
  Operator view: unresolved alert issues per rule key, across all identifiers.
  """
  @spec open_counts(User.t()) :: {:ok, %{String.t() => pos_integer()}} | {:error, term()}
  def open_counts(%User{} = user) do
    with {:ok, rows} <- list_active(user) do
      {:ok, Enum.frequencies_by(rows, & &1["rule_key"])}
    end
  end

  @doc """
  Internal: the latest version of every unresolved alert issue, across identifiers.
  """
  @spec list_active(User.t()) :: {:ok, [map()]} | {:error, term()}
  def list_active(%User{} = user) do
    with {:ok, rows} <- latest(user, [], [], nil, @max_limit) do
      {:ok, Enum.reject(rows, &(&1["status"] == "resolved"))}
    end
  end

  @doc """
  Internal: the `version` (µs) of the latest notification delivery logged for each
  `{alert_issue_id, policy_name}` among `alert_issue_ids`.
  """
  @spec last_notifications(User.t(), [String.t()]) ::
          {:ok, %{{String.t(), String.t()} => integer()}} | {:error, term()}
  def last_notifications(%User{}, []), do: {:ok, %{}}

  def last_notifications(%User{} = user, alert_issue_ids) do
    sql = """
    select distinct on (body ->> 'alert_issue_id', body ->> 'policy_name') body::text as body_json
    from %{table}
    where body ->> 'alert_issue_id' = any($1)
      and timestamp >= $2
    order by body ->> 'alert_issue_id', body ->> 'policy_name', cast(body ->> 'version' as bigint) desc
    """

    with {:ok, rows} <-
           system_query(
             user,
             :alert_issue_notifications,
             sql,
             [alert_issue_ids, retention_floor()],
             []
           ) do
      {:ok, Map.new(rows, &{{&1["alert_issue_id"], &1["policy_name"]}, &1["version"]})}
    end
  end

  defp run(user, opts, alert_issue_id) do
    fields = if alert_issue_id, do: [{"alert_issue_id", alert_issue_id}], else: []

    with {:ok, filters} <- validate_filters(Keyword.get(opts, :filters, %{})) do
      limit = opts |> Keyword.get(:limit, @max_limit) |> min(@max_limit)
      latest(user, filters, fields, Keyword.get(opts, :since), limit)
    end
  end

  defp latest(user, filters, fields, since, limit) do
    case alert_issues_storage(user) do
      {:ok, source, backend} ->
        {sql, params} =
          latest_sql(
            PostgresAdaptor.table_name(source),
            filters,
            fields,
            clamp_since(since),
            limit
          )

        backend
        |> PostgresAdaptor.execute_query({sql, params}, [])
        |> decode_bodies([], &Function.identity/1)

      {:error, :no_alert_issues} ->
        {:ok, []}

      {:error, _} = error ->
        error
    end
  end

  defp alert_issues_storage(user) do
    with %_{} = source <-
           Sources.Cache.get_by(user_id: user.id, system_source_type: :alert_issues) ||
             {:error, :no_alert_issues},
         {:ok, backend} <- postgres_backend(user, source) do
      {:ok, source, backend}
    end
  end

  @doc false
  @spec postgres_backend(User.t(), Logflare.Sources.Source.t()) ::
          {:ok, Logflare.Backends.Backend.t()} | {:error, :unsupported_backend}
  def postgres_backend(user, source) do
    case Backends.list_backends(source_id: source.id, type: :postgres) do
      [backend | _] ->
        {:ok, backend}

      [] ->
        case Backends.get_default_backend(user) do
          %{type: :postgres} = backend -> {:ok, backend}
          _ -> {:error, :unsupported_backend}
        end
    end
  end

  defp decode_bodies({:ok, %{rows: rows}}, _empty, collect),
    do: {:ok, rows |> Enum.map(&Jason.decode!(&1["body_json"])) |> collect.()}

  defp decode_bodies({:error, error}, empty, _collect) do
    if missing_table?(error), do: {:ok, empty}, else: {:error, error}
  end

  defp missing_table?(%QueryError{
         raw_error: %Postgrex.Error{postgres: %{code: :undefined_table}}
       }),
       do: true

  defp missing_table?(_error), do: false

  defp validate_filters(filters) when map_size(filters) == 0, do: {:error, :filters_required}

  defp validate_filters(filters) when is_map(filters) do
    Enum.reduce_while(filters, {:ok, []}, fn {key, value}, {:ok, acc} ->
      if is_binary(key) and Regex.match?(@filter_key, key) do
        {:cont, {:ok, [{key, to_string(value)} | acc]}}
      else
        {:halt, {:error, {:invalid_filter_key, key}}}
      end
    end)
  end

  defp clamp_since(nil), do: retention_floor()

  defp clamp_since(%DateTime{} = since) do
    floor = retention_floor()
    if DateTime.compare(since, floor) == :lt, do: floor, else: since
  end

  defp retention_floor, do: DateTime.add(DateTime.utc_now(), -@retention_days, :day)

  defp latest_sql(table, filters, fields, since, limit) do
    {filter_sql, params} =
      filters
      |> Enum.sort()
      |> Enum.map_reduce([since], fn {key, value}, acc ->
        n = length(acc)
        {"and body -> 'identifiers' ->> $#{n + 1} = $#{n + 2}", acc ++ [key, value]}
      end)

    {field_sql, params} =
      Enum.map_reduce(fields, params, fn {key, value}, acc ->
        n = length(acc)
        {"and body ->> $#{n + 1} = $#{n + 2}", acc ++ [key, value]}
      end)

    sql = """
    select body::text as body_json from (
      select distinct on (body ->> 'alert_issue_id') body
      from #{table}
      where timestamp >= $1
        #{Enum.join(filter_sql ++ field_sql, "\n    ")}
      order by body ->> 'alert_issue_id', cast(body ->> 'version' as bigint) desc
    ) latest
    order by body ->> 'last_seen_at' desc
    limit #{limit}
    """

    {sql, params}
  end
end
