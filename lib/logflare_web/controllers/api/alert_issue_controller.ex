defmodule LogflareWeb.Api.AlertIssueController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues.Queries
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.AlertIssueApiSchema

  @identifier_prefix "identifiers."

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["management"])

  @filter_params [
    filters: [
      in: :query,
      description: "Identifier filters, e.g. filters[identifiers.project]=abcd1234",
      type: :object,
      style: :deepObject,
      required: true
    ],
    status: [in: :query, description: "Comma-separated statuses", type: :string],
    since: [in: :query, description: "ISO 8601 lower bound, at most 30 days back", type: :string]
  ]

  operation(:index,
    summary: "List alert issues",
    parameters: @filter_params,
    responses: %{200 => List.response(AlertIssueApiSchema), 400 => BadRequest.response()}
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    with {:ok, opts} <- query_opts(params),
         {:ok, issues} <- Queries.list(user, opts) |> map_error() do
      json(conn, issues)
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  operation(:show,
    summary: "Fetch alert issue",
    parameters:
      [alert_issue_id: [in: :path, description: "Alert issue ID", type: :string]] ++
        Keyword.take(@filter_params, [:filters]),
    responses: %{
      200 => AlertIssueApiSchema.response(),
      400 => BadRequest.response(),
      404 => NotFound.response()
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"alert_issue_id" => alert_issue_id} = params) do
    with {:ok, opts} <- query_opts(params),
         {:ok, issue} <- Queries.get(user, alert_issue_id, opts) |> map_error() do
      json(conn, issue)
    else
      {:error, :not_found} -> {:error, :not_found}
      {:error, message} -> bad_request(conn, message)
    end
  end

  defp query_opts(params) do
    with {:ok, filters} <- parse_filters(Map.get(params, "filters")),
         {:ok, since} <- parse_since(Map.get(params, "since")) do
      opts =
        [filters: filters, statuses: parse_statuses(Map.get(params, "status")), since: since]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)

      {:ok, opts}
    end
  end

  defp parse_filters(filters) when is_map(filters) and map_size(filters) > 0 do
    Enum.reduce_while(filters, {:ok, %{}}, fn
      {@identifier_prefix <> key, value}, {:ok, acc} when is_binary(value) ->
        {:cont, {:ok, Map.put(acc, key, value)}}

      {key, _value}, _acc ->
        {:halt, {:error, "unsupported filter: #{key}"}}
    end)
  end

  defp parse_filters(_filters), do: {:error, "filters[identifiers.<key>] is required"}

  defp parse_since(nil), do: {:ok, nil}

  defp parse_since(value) do
    case DateTime.from_iso8601(value) do
      {:ok, since, _offset} -> {:ok, since}
      {:error, _} -> {:error, "since must be an ISO 8601 timestamp"}
    end
  end

  defp parse_statuses(nil), do: nil
  defp parse_statuses(value), do: String.split(value, ",", trim: true)

  defp map_error({:error, :filters_required}),
    do: {:error, "filters[identifiers.<key>] is required"}

  defp map_error({:error, {:invalid_filter_key, key}}), do: {:error, "invalid filter key: #{key}"}
  defp map_error(result), do: result

  defp bad_request(conn, message) do
    conn
    |> put_status(400)
    |> json(%{error: message})
  end
end
