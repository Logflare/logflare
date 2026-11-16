defmodule Logflare.AlertIssues.Queries do
  @moduledoc """
  Code-defined read queries over `system.alert_issues`.

  The SQL lives here rather than in user-visible endpoints. Queries are scoped by
  generic identifier filters (`filters[identifiers.<key>]=<value>`): keys must match
  `#{inspect(~r/^[a-z0-9_]{1,64}$/)}` and keys and values are bound as parameters.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.QueryError
  alias Logflare.Sources
  alias Logflare.User

  @retention_days 30
  @max_limit 500
  @filter_key ~r/^[a-z0-9_]{1,64}$/

  @type opt ::
          {:filters, %{optional(String.t()) => String.t()}}
          | {:statuses, [String.t()]}
          | {:since, DateTime.t()}
          | {:limit, pos_integer()}
          | {:reader, String.t()}

  @spec list(User.t(), [opt()]) :: {:ok, [map()]} | {:error, term()}
  def list(%User{} = user, opts) do
    with {:ok, rows} <- run(user, opts, nil) do
      statuses = Keyword.get(opts, :statuses)
      rows = Enum.filter(rows, &(is_nil(statuses) or &1["status"] in statuses))
      put_unread(user, rows, Keyword.get(opts, :reader))
    end
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

  defp run(user, opts, alert_issue_id) do
    with {:ok, filters} <- validate_filters(Keyword.get(opts, :filters, %{})),
         {:ok, source, backend} <- alert_issues_storage(user) do
      since = clamp_since(Keyword.get(opts, :since))
      limit = opts |> Keyword.get(:limit, @max_limit) |> min(@max_limit)

      {sql, params} =
        latest_sql(PostgresAdaptor.table_name(source), filters, since, alert_issue_id, limit)

      backend
      |> PostgresAdaptor.execute_query({sql, params}, [])
      |> decode_bodies([], &Function.identity/1)
    else
      {:error, :no_alert_issues} -> {:ok, []}
      {:error, _} = error -> error
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

  defp latest_sql(table, filters, since, alert_issue_id, limit) do
    params = [since]

    {filter_sql, params} =
      filters
      |> Enum.sort()
      |> Enum.map_reduce(params, fn {key, value}, acc ->
        n = length(acc)
        {"and body -> 'identifiers' ->> $#{n + 1} = $#{n + 2}", acc ++ [key, value]}
      end)

    {id_sql, params} =
      case alert_issue_id do
        nil -> {"", params}
        id -> {"and body ->> 'alert_issue_id' = $#{length(params) + 1}", params ++ [id]}
      end

    sql = """
    select body::text as body_json from (
      select distinct on (body ->> 'alert_issue_id') body
      from #{table}
      where timestamp >= $1
        #{Enum.join(filter_sql, "\n    ")}
        #{id_sql}
      order by body ->> 'alert_issue_id', cast(body ->> 'version' as bigint) desc
    ) latest
    order by body ->> 'last_seen_at' desc
    limit #{limit}
    """

    {sql, params}
  end
end
