defmodule Logflare.AlertIssues.Queries do
  @moduledoc """
  Code-defined read queries over `system.alert_issues`.

  The SQL lives here rather than in user-visible endpoints. Queries are scoped by
  generic identifier filters (`filters[identifiers.<key>]=<value>`): keys must match
  `#{inspect(~r/^[a-z0-9_]{1,64}$/)}` and keys and values are bound as parameters.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
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

  @spec list(User.t(), [opt()]) :: {:ok, [map()]} | {:error, term()}
  def list(%User{} = user, opts) do
    with {:ok, rows} <- run(user, opts, nil) do
      statuses = Keyword.get(opts, :statuses)
      {:ok, Enum.filter(rows, &(is_nil(statuses) or &1["status"] in statuses))}
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

      case PostgresAdaptor.execute_query(backend, {sql, params}, []) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &Jason.decode!(&1["body_json"]))}
        {:error, _} = error -> error
      end
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

  defp postgres_backend(user, source) do
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
