defmodule LogflareWeb.Api.AlertIssueController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.Queries
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.AlertIssueApiSchema

  @identifier_prefix "identifiers."
  @actions %{
    "acknowledge" => :acknowledge,
    "snooze" => :snooze,
    "resolve" => :resolve,
    "reopen" => :reopen
  }

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
    since: [in: :query, description: "ISO 8601 lower bound, at most 30 days back", type: :string],
    reader: [in: :query, description: "Adds an unread flag for this reader", type: :string]
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

  operation(:apply_action,
    summary: "Apply an action to an alert issue",
    parameters: [alert_issue_id: [in: :path, description: "Alert issue ID", type: :string]],
    request_body:
      {"Action", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:action, :filters],
         properties: %{
           action: %OpenApiSpex.Schema{type: :string, enum: Map.keys(@actions)},
           filters: %OpenApiSpex.Schema{type: :object},
           actor: %OpenApiSpex.Schema{type: :string},
           until: %OpenApiSpex.Schema{type: :string, format: :"date-time"}
         }
       }},
    responses: %{
      200 => AlertIssueApiSchema.response(),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      409 => {"Invalid transition", "application/json", BadRequest.schema()}
    }
  )

  def apply_action(
        %{assigns: %{user: user}} = conn,
        %{"alert_issue_id" => alert_issue_id} = params
      ) do
    with {:ok, action} <- parse_action(Map.get(params, "action")),
         {:ok, filters} <- parse_filters(Map.get(params, "filters")),
         {:ok, until} <- parse_since(Map.get(params, "until")) do
      opts =
        [filters: filters, actor: Map.get(params, "actor") || user.email, until: until]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)

      case AlertIssues.act(user, alert_issue_id, action, opts) do
        {:ok, issue} -> json(conn, issue)
        {:error, :not_found} -> {:error, :not_found}
        {:error, :invalid_transition} -> error(conn, 409, "invalid transition for #{action}")
        {:error, :until_required} -> bad_request(conn, "until is required to snooze")
        {:error, reason} -> bad_request(conn, inspect(reason))
      end
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  operation(:reads,
    summary: "Mark alert issues read or unread",
    request_body:
      {"Reads", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:alert_issue_ids, :reader, :filters],
         properties: %{
           alert_issue_ids: %OpenApiSpex.Schema{
             type: :array,
             items: %OpenApiSpex.Schema{type: :string}
           },
           reader: %OpenApiSpex.Schema{type: :string},
           read: %OpenApiSpex.Schema{type: :boolean, default: true},
           filters: %OpenApiSpex.Schema{type: :object}
         }
       }},
    responses: %{
      200 =>
        {"Count", "application/json",
         %OpenApiSpex.Schema{
           type: :object,
           properties: %{count: %OpenApiSpex.Schema{type: :integer}}
         }},
      400 => BadRequest.response(),
      404 => NotFound.response()
    }
  )

  def reads(%{assigns: %{user: user}} = conn, params) do
    with {:ok, filters} <- parse_filters(Map.get(params, "filters")),
         {:ok, ids, reader, read} <- parse_reads(params) do
      case AlertIssues.mark_read(user, ids, reader, read, filters: filters) do
        {:ok, count} -> json(conn, %{count: count})
        {:error, :not_found} -> {:error, :not_found}
        {:error, reason} -> bad_request(conn, inspect(reason))
      end
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  defp parse_reads(%{"alert_issue_ids" => [_ | _] = ids, "reader" => reader} = params)
       when is_binary(reader) do
    if Enum.all?(ids, &is_binary/1),
      do: {:ok, ids, reader, Map.get(params, "read", true) in [true, "true"]},
      else: {:error, "alert_issue_ids must be strings"}
  end

  defp parse_reads(_params), do: {:error, "alert_issue_ids and reader are required"}

  defp parse_action(action) do
    case Map.fetch(@actions, action) do
      {:ok, action} -> {:ok, action}
      :error -> {:error, "action must be one of #{Enum.join(Map.keys(@actions), ", ")}"}
    end
  end

  defp query_opts(params) do
    with {:ok, filters} <- parse_filters(Map.get(params, "filters")),
         {:ok, since} <- parse_since(Map.get(params, "since")) do
      opts =
        [
          filters: filters,
          statuses: parse_statuses(Map.get(params, "status")),
          since: since,
          reader: Map.get(params, "reader")
        ]
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

  defp bad_request(conn, message), do: error(conn, 400, message)

  defp error(conn, status, message) do
    conn
    |> put_status(status)
    |> json(%{error: message})
  end
end
