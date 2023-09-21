defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Logs.IngestTypecasting
  alias Logflare.Backends

  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.ServerError
  alias LogflareWeb.OpenApiSchemas.LogsCreated

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["Public"])

  plug(
    CORSPlug,
    [
      origin: "*",
      max_age: 1_728_000,
      headers: [
        "Authorization",
        "Content-Type",
        "Content-Length",
        "X-Requested-With"
      ],
      methods: ["POST", "OPTIONS"],
      send_preflight_response?: true
    ]
    when action in [:browser_reports, :generic_json, :create]
  )

  alias Logflare.Logs

  @message "Logged!"

  operation(:create,
    summary: "Create log event",
    description:
      "Full details are available in the [ingestion documentation](https://docs.logflare.app/concepts/ingestion/)",
    parameters: [
      source: [
        in: :query,
        description: "Source UUID",
        type: :string,
        example: "a040ae88-3e27-448b-9ee6-622278b23193",
        required: false
      ],
      source_name: [
        in: :query,
        description: "Source name",
        type: :string,
        example: "MyApp.MySource",
        required: false
      ]
    ],
    responses: %{
      201 => Created.response(LogsCreated),
      500 => ServerError.response()
    }
  )

  def create(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def create(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def create(%{assigns: %{source: source}} = conn, _slog_params) do
    log_params = Map.drop(conn.body_params, ["timestamp", "id"])
    ingest_and_render(conn, [log_params], source)
  end

  def cloudflare(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def cloudflare(%{assigns: %{source: source}} = conn, log_params) when is_map(log_params) do
    log_params = Map.drop(log_params, ["source", "timestamp", "id"])
    ingest_and_render(conn, [log_params], source)
  end

  def syslog(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def generic_json(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch = Logs.GenericJson.handle_batch(batch)

    ingest_and_render(conn, batch, source)
  end

  def generic_json(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    batch =
      event
      |> List.wrap()
      |> Logs.GenericJson.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def vector(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch = Logs.Vector.handle_batch(batch)

    ingest_and_render(conn, batch, source)
  end

  def vector(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    batch =
      event
      |> List.wrap()
      |> Logs.Vector.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def browser_reports(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch = Logs.BrowserReport.handle_batch(batch)

    ingest_and_render(conn, batch, source)
  end

  def browser_reports(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    batch =
      event
      |> List.wrap()
      |> Logs.BrowserReport.handle_batch()

    ingest_and_render(conn, batch, source)
  end

  def elixir_logger(%{assigns: %{source: source}} = conn, %{"batch" => batch})
      when is_list(batch) do
    ingest_and_render(conn, batch, source)
  end

  def create_with_typecasts(%{assigns: %{source: source}} = conn, %{"batch" => batch})
      when is_list(batch) do
    batch = IngestTypecasting.maybe_cast_batch(batch)

    ingest_and_render(conn, batch, source)
  end

  def vercel_ingest(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch = Logs.Vercel.handle_batch(batch, source)

    ingest_and_render(conn, batch, source)
  end

  def netlify(%{assigns: %{source: source}} = conn, %{"_json" => batch}) when is_list(batch) do
    batch = Logs.Netlify.handle_batch(batch, source)

    ingest_and_render(conn, batch, source)
  end

  def netlify(%{assigns: %{source: source}, body_params: params} = conn, _params)
      when is_map(params) do
    batch = Logs.Netlify.handle_batch([params], source)

    ingest_and_render(conn, batch, source)
  end

  def github(%{assigns: %{source: source}, body_params: params} = conn, _params) do
    batch = Logs.Github.handle_batch([params], source)

    ingest_and_render(conn, batch, source)
  end

  def ingest_and_render(conn, log_params_batch, source) do
    result =
      if source.v2_pipeline do
        Backends.start_source_sup(source)
        Backends.ingest_logs(log_params_batch, source)
      else
        Logs.ingest_logs(log_params_batch, source)
      end

    case result do
      :ok ->
        render(conn, "index.json", message: @message)

      {:error, errors} ->
        conn
        |> put_status(406)
        |> put_view(LogflareWeb.LogView)
        |> render("index.json", message: errors)
    end
  end
end
