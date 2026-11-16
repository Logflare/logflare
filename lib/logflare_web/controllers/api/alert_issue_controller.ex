defmodule LogflareWeb.Api.AlertIssueController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.LogEvents
  alias Logflare.AlertIssues.Queries
  alias Logflare.Backends.QueryError
  alias LogflareWeb.Api.AlertIssueFilters
  alias LogflareWeb.OpenApi.BadRequest
  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.List
  alias LogflareWeb.OpenApi.NotFound
  alias LogflareWeb.OpenApiSchemas.AlertIssueApiSchema
  alias LogflareWeb.OpenApiSchemas.AlertIssueCommentApiSchema
  alias LogflareWeb.OpenApiSchemas.AlertLogEventsApiSchema

  @backend_error {"Backend query error", "application/json", BadRequest.schema()}
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
    reader: [in: :query, description: "Adds an unread flag for this reader", type: :string],
    include_silenced: [
      in: :query,
      description: "Include alert issues hidden by a silence",
      type: :boolean
    ]
  ]

  operation(:index,
    summary: "List alert issues",
    parameters: @filter_params,
    responses: %{
      200 => List.response(AlertIssueApiSchema),
      400 => BadRequest.response(),
      500 => @backend_error
    }
  )

  def index(%{assigns: %{user: user}} = conn, params) do
    with {:ok, opts} <- query_opts(params),
         {:ok, issues} <- Queries.list(user, opts) |> map_error() do
      json(conn, issues)
    else
      {:error, %QueryError{} = error} -> backend_error(conn, error)
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
      404 => NotFound.response(),
      500 => @backend_error
    }
  )

  def show(%{assigns: %{user: user}} = conn, %{"alert_issue_id" => alert_issue_id} = params) do
    with {:ok, opts} <- query_opts(params),
         {:ok, issue} <- Queries.get(user, alert_issue_id, opts) |> map_error() do
      json(conn, issue)
    else
      {:error, :not_found} -> {:error, :not_found}
      {:error, %QueryError{} = error} -> backend_error(conn, error)
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
      409 => {"Invalid transition", "application/json", BadRequest.schema()},
      500 => @backend_error
    }
  )

  def apply_action(
        %{assigns: %{user: user}} = conn,
        %{"alert_issue_id" => alert_issue_id} = params
      ) do
    with {:ok, action} <- parse_action(Map.get(params, "action")),
         {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")),
         {:ok, until} <- parse_since(Map.get(params, "until")) do
      opts =
        [filters: filters, actor: Map.get(params, "actor") || user.email, until: until]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)

      case AlertIssues.act(user, alert_issue_id, action, opts) do
        {:ok, issue} -> json(conn, issue)
        {:error, :not_found} -> {:error, :not_found}
        {:error, :invalid_transition} -> error(conn, 409, "invalid transition for #{action}")
        {:error, :until_required} -> bad_request(conn, "until is required to snooze")
        {:error, %QueryError{} = error} -> backend_error(conn, error)
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
      404 => NotFound.response(),
      500 => @backend_error
    }
  )

  def reads(%{assigns: %{user: user}} = conn, params) do
    with {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")),
         {:ok, ids, reader, read} <- parse_reads(params) do
      case AlertIssues.mark_read(user, ids, reader, read, filters: filters) do
        {:ok, count} -> json(conn, %{count: count})
        {:error, :not_found} -> {:error, :not_found}
        {:error, %QueryError{} = error} -> backend_error(conn, error)
        {:error, reason} -> bad_request(conn, inspect(reason))
      end
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  operation(:comments,
    summary: "List alert issue comments",
    parameters:
      [alert_issue_id: [in: :path, description: "Alert issue ID", type: :string]] ++
        Keyword.take(@filter_params, [:filters]),
    responses: %{
      200 => List.response(AlertIssueCommentApiSchema),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      500 => @backend_error
    }
  )

  def comments(%{assigns: %{user: user}} = conn, %{"alert_issue_id" => alert_issue_id} = params) do
    with {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")) do
      case Queries.comments(user, alert_issue_id, filters: filters) do
        {:ok, comments} -> json(conn, comments)
        {:error, :not_found} -> {:error, :not_found}
        {:error, %QueryError{} = error} -> backend_error(conn, error)
        {:error, reason} -> bad_request(conn, inspect(reason))
      end
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  operation(:create_comment,
    summary: "Comment on an alert issue",
    parameters:
      [alert_issue_id: [in: :path, description: "Alert issue ID", type: :string]] ++
        Keyword.take(@filter_params, [:filters]),
    request_body:
      {"Comment", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:body],
         properties: %{
           body: %OpenApiSpex.Schema{type: :string, maxLength: 5000},
           author: %OpenApiSpex.Schema{type: :string}
         }
       }},
    responses: %{
      201 => Created.response(AlertIssueCommentApiSchema),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      422 => {"Invalid comment", "application/json", BadRequest.schema()},
      500 => @backend_error
    }
  )

  def create_comment(
        %{assigns: %{user: user}} = conn,
        %{"alert_issue_id" => alert_issue_id} = params
      ) do
    with {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")) do
      author = Map.get(params, "author") || user.email

      case AlertIssues.comment(user, alert_issue_id, to_string(Map.get(params, "body", "")),
             author: author,
             filters: filters
           ) do
        {:ok, comment} -> conn |> put_status(201) |> json(comment)
        {:error, :not_found} -> {:error, :not_found}
        {:error, :invalid_body} -> error(conn, 422, "body must be 1 to 5000 characters")
        {:error, %QueryError{} = error} -> backend_error(conn, error)
        {:error, reason} -> bad_request(conn, inspect(reason))
      end
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  @range_params [
    from: [
      in: :query,
      description: "ISO 8601 start, defaults to 24 hours before to",
      type: :string
    ],
    to: [in: :query, description: "ISO 8601 end, defaults to now", type: :string]
  ]
  @preview_error {"No log preview", "application/json", BadRequest.schema()}

  operation(:events,
    summary: "Preview the log events behind an alert issue",
    parameters:
      [alert_issue_id: [in: :path, description: "Alert issue ID", type: :string]] ++
        Keyword.take(@filter_params, [:filters]) ++ @range_params,
    responses: %{
      200 => AlertLogEventsApiSchema.response(),
      400 => BadRequest.response(),
      404 => NotFound.response(),
      422 => @preview_error,
      500 => @backend_error
    }
  )

  def events(%{assigns: %{user: user}} = conn, %{"alert_issue_id" => alert_issue_id} = params) do
    with {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")),
         {:ok, range} <- parse_range(params) do
      user
      |> LogEvents.for_alert_issue(alert_issue_id, [filters: filters] ++ range)
      |> preview_response(conn)
    else
      {:error, message} -> bad_request(conn, message)
    end
  end

  operation(:preview,
    summary: "Preview the log events matching an LQL filter",
    request_body:
      {"Preview", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         required: [:lql],
         properties: %{
           lql: %OpenApiSpex.Schema{type: :string},
           from: %OpenApiSpex.Schema{type: :string, format: :"date-time"},
           to: %OpenApiSpex.Schema{type: :string, format: :"date-time"}
         }
       }},
    responses: %{
      200 => AlertLogEventsApiSchema.response(),
      400 => BadRequest.response(),
      422 => @preview_error,
      500 => @backend_error
    }
  )

  def preview(%{assigns: %{user: user}} = conn, %{"lql" => lql} = params) when is_binary(lql) do
    case parse_range(params) do
      {:ok, range} -> user |> LogEvents.preview(lql, range) |> preview_response(conn)
      {:error, message} -> bad_request(conn, message)
    end
  end

  def preview(conn, _params), do: bad_request(conn, "lql is required")

  defp preview_response({:ok, result}, conn), do: json(conn, result)
  defp preview_response({:error, :not_found}, _conn), do: {:error, :not_found}

  defp preview_response({:error, %QueryError{} = error}, conn), do: backend_error(conn, error)

  defp preview_response({:error, reason}, conn),
    do: error(conn, 422, preview_error_message(reason))

  defp preview_error_message(:no_preview), do: "this alert issue type has no log preview"
  defp preview_error_message(:source_required), do: "lql must name a source with f:<source>"
  defp preview_error_message({:unknown_source, name}), do: "unknown source: #{name}"
  defp preview_error_message({:missing_identifier, key}), do: "missing identifier: #{key}"
  defp preview_error_message({:invalid_identifier, key}), do: "invalid identifier: #{key}"
  defp preview_error_message({:invalid_lql, _reason}), do: "invalid lql"
  defp preview_error_message({:unsupported_operator, op}), do: "unsupported lql operator: #{op}"
  defp preview_error_message(:unsupported_backend), do: "source is not on a postgres backend"
  defp preview_error_message(reason), do: inspect(reason)

  defp parse_range(params) do
    with {:ok, from} <- parse_time(Map.get(params, "from"), "from"),
         {:ok, to} <- parse_time(Map.get(params, "to"), "to") do
      {:ok, [from: from, to: to]}
    end
  end

  defp parse_time(nil, _name), do: {:ok, nil}

  defp parse_time(value, name) do
    case DateTime.from_iso8601(value) do
      {:ok, time, _offset} -> {:ok, time}
      {:error, _} -> {:error, "#{name} must be an ISO 8601 timestamp"}
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
    with {:ok, filters} <- AlertIssueFilters.parse(Map.get(params, "filters")),
         {:ok, since} <- parse_since(Map.get(params, "since")) do
      opts =
        [
          filters: filters,
          statuses: parse_statuses(Map.get(params, "status")),
          since: since,
          reader: Map.get(params, "reader"),
          include_silenced: Map.get(params, "include_silenced") in [true, "true"]
        ]
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)

      {:ok, opts}
    end
  end

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

  defp backend_error(conn, %QueryError{description: description, kind: kind}),
    do: error(conn, 500, description || "backend query failed: #{kind}")

  defp error(conn, status, message) do
    conn
    |> put_status(status)
    |> json(%{error: message})
  end
end
