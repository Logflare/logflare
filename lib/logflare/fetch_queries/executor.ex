defmodule Logflare.FetchQueries.Executor do
  @moduledoc """
  Executes fetch queries against backends.
  Supports webhook (HTTP) and bigquery backends.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.FetchQueries.FetchQuery

  require Logger

  @doc """
  Executes a fetch query against its backend.
  Returns {:ok, [event_maps]} or {:error, reason}
  """
  @spec execute(FetchQuery.t()) ::
          {:ok,
           %{
             rows: [map()],
             total_bytes_processed: integer(),
             query_string: string(),
             total_rows: integer()
           }}
          | {:error, term()}
  def execute(%FetchQuery{backend: nil, user: user} = fetch_query) when user != nil do
    # Use system default backend if no backend is configured
    default_backend = Backends.get_default_backend(user)
    execute(%{fetch_query | backend: default_backend})
  end

  def execute(%FetchQuery{backend: nil}) do
    {:error, "Fetch query has no backend configured and user is not loaded"}
  end

  def execute(%FetchQuery{backend: %{type: :webhook}} = fetch_query) do
    with {:ok, raw_data} <- execute_http(fetch_query),
         events <- apply_jsonpath_if_needed(raw_data, fetch_query) do
      {:ok,
       %{
         rows: events,
         total_bytes_processed: 0,
         query_string: fetch_query.query,
         total_rows: length(events)
       }}
    end
  end

  def execute(%FetchQuery{backend: %{type: :bigquery}} = fetch_query) do
    execute_bigquery(fetch_query)
  end

  def execute(%FetchQuery{backend: %{type: type}}) do
    {:error, "Backend type #{type} not supported for fetch queries"}
  end

  defp execute_http(%FetchQuery{backend: backend, query: query, language: language}) do
    config = backend.config || %{}

    url =
      case {query, language} do
        {nil, _} -> config["url"]
        {"", _} -> config["url"]
        {_, :jsonpath} -> config["url"]
        {url_str, _} -> url_str
      end

    unless url do
      raise "Webhook backend must have a URL configured"
    end

    headers = config["headers"] || %{}
    token = config["bearer_token"]
    gzip = Map.get(config, "gzip", true)
    http = Map.get(config, "http", "http2")

    client =
      Logflare.Backends.Adaptor.HttpBased.Client.new(
        url: url,
        headers: headers,
        token: token,
        gzip: gzip,
        http2: http == "http2",
        json: true
      )

    case Tesla.get(client, "") do
      {:ok, %{status: status, body: body}} when status in 200..299 ->
        {:ok, ensure_list(body)}

      {:ok, %{status: status}} ->
        {:error, "HTTP #{status}"}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp execute_bigquery(%FetchQuery{backend: backend, query: sql}) do
    case BigQueryAdaptor.execute_query(backend, sql, []) |> dbg() do
      {:ok, %{rows: rows}} = result when is_list(rows) ->
        result

      {:error, reason} ->
        {:error, reason}

      other ->
        Logger.warning("Unexpected response from BigQueryAdaptor.execute_query #{inspect(other)}",
          error_string: inspect(other)
        )

        {:error, "Unexpected response from BigQuery"}
    end
  end

  defp apply_jsonpath_if_needed(data, %FetchQuery{language: :jsonpath, query: jsonpath})
       when is_binary(jsonpath) and jsonpath != "" do
    Enum.flat_map(ensure_list(data), fn item ->
      case Warpath.query(item, jsonpath) do
        {:ok, results} when is_list(results) ->
          Enum.map(results, &ensure_map/1)

        {:ok, result} ->
          [ensure_map(result)]

        {:error, _} ->
          [ensure_map(item)]
      end
    end)
  end

  defp apply_jsonpath_if_needed(data, _fetch_query) do
    ensure_list(data) |> Enum.map(&ensure_map/1)
  end

  defp ensure_list(val) when is_list(val), do: val
  defp ensure_list(val), do: [val]

  defp ensure_map(val) when is_map(val), do: val
  defp ensure_map(val), do: %{"data" => val}
end
