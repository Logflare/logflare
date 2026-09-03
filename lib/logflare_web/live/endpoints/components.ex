defmodule LogflareWeb.Endpoints.Components do
  use LogflareWeb, :html
  use Phoenix.Component

  alias Logflare.Endpoints.PiiRedactor
  alias LogflareWeb.QueryComponents

  @field_labels [
    token: "ID",
    enable_auth: "Authentication",
    max_limit: "Max rows",
    cache_duration_seconds: "Caching",
    proactive_requerying_seconds: "Cache warming",
    sandboxable: "Query sandboxing",
    redact_pii: "Redact PII",
    enable_dynamic_reservation: "Dynamic reservation"
  ]

  attr :endpoint, :map, required: true

  def authentication_warning(assigns) do
    ~H"""
    <div :if={not @endpoint.enable_auth}>
      <.alert variant="warning">
        <strong>Authentication not enabled!</strong>
        <br />
        <span>
          Authentication has not been enabled for this endpoint, and may pose a security risk.
        </span>
      </.alert>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :close_event, :string, default: nil

  def endpoint_snapshot_header(assigns) do
    ~H"""
    <div class="tw-border-0 tw-bg-emerald-400 tw-px-8 tw-py-6 tw-text-zinc-950">
      <div class="tw-flex tw-min-w-0 tw-flex-1 tw-items-start tw-justify-between tw-gap-4">
        <div class="tw-min-w-0">
          <h5 class="modal-title tw-mb-0 tw-truncate tw-text-2xl tw-font-semibold tw-tracking-tight tw-text-zinc-950">
            {@title}
          </h5>
          <div :if={@subtitle} class="tw-mt-2 tw-flex tw-flex-wrap tw-gap-x-3 tw-gap-y-1 tw-text-sm tw-text-zinc-900">
            {@subtitle}
          </div>
        </div>

        <span :if={@close_event}>
          <a href="#" phx-click={@close_event} class="phx-modal-close tw-text-2xl tw-leading-none tw-text-zinc-950 tw-no-underline">&times;</a>
        </span>
      </div>
    </div>
    """
  end

  attr :endpoint, :map, required: true

  def endpoint_settings_panel(assigns) do
    ~H"""
    <section class="tw-rounded tw-border tw-bg-dashboard-grey tw-p-5">
      <div class="tw-grid tw-gap-x-14 tw-gap-y-0 md:tw-grid-cols-2">
        <.setting_row label={:enable_auth} value={format_toggle(@endpoint.enable_auth)} />
        <.setting_row label={:max_limit} value={to_string(@endpoint.max_limit)} />
        <.setting_row label={:cache_duration_seconds} value={format_cache_duration(@endpoint.cache_duration_seconds)} />
        <.setting_row :if={(@endpoint.cache_duration_seconds || 0) > 0} label={:proactive_requerying_seconds} value={format_cache_duration(@endpoint.proactive_requerying_seconds)} />
        <.setting_row label={:sandboxable} value={format_toggle(@endpoint.sandboxable)} />
        <.setting_row label={:redact_pii} value={format_toggle(@endpoint.redact_pii)} />
        <.setting_row label={:enable_dynamic_reservation} value={format_toggle(@endpoint.enable_dynamic_reservation)} />
        <.setting_row :if={@endpoint.labels} label={:labels} value={@endpoint.labels} />
      </div>
    </section>
    """
  end

  attr :endpoint, :map, required: true
  attr :parsed_result, :map, default: nil
  attr :redact_pii, :boolean, default: false

  def endpoint_details(assigns) do
    ~H"""
    <div id="endpoint-details" class="tw-flex tw-min-w-0 tw-flex-col tw-gap-8">
      <p class="tw-mb-0 tw-whitespace-pre-wrap tw-text-sm tw-text-zinc-400">{@endpoint.description || "—"}</p>

      <.endpoint_query_tabs endpoint={@endpoint} parsed_result={@parsed_result} redact_pii={@redact_pii} />

      <section>
        <h3 class="tw-mb-3 tw-text-xl tw-font-semibold tw-text-white">Properties</h3>
        <.endpoint_properties endpoint={@endpoint} />
      </section>
    </div>
    """
  end

  attr :endpoint, :map, required: true

  defp endpoint_properties(assigns) do
    ~H"""
    <section class="tw-rounded tw-border tw-bg-dashboard-grey tw-p-5">
      <.property_row label={:token} value={@endpoint.token} />
      <.property_row label={:enable_auth} value={format_toggle(@endpoint.enable_auth)} />
      <.property_row label={:max_limit} value={to_string(@endpoint.max_limit)} />
      <.property_row label={:cache_duration_seconds} value={format_cache_duration(@endpoint.cache_duration_seconds)} />
      <.property_row :if={(@endpoint.cache_duration_seconds || 0) > 0} label={:proactive_requerying_seconds} value={format_cache_duration(@endpoint.proactive_requerying_seconds)} />
      <.property_row label={:sandboxable} value={format_toggle(@endpoint.sandboxable)} />
      <.property_row label={:redact_pii} value={format_toggle(@endpoint.redact_pii)} />
      <.property_row label={:enable_dynamic_reservation} value={format_toggle(@endpoint.enable_dynamic_reservation)} />
      <.property_row :if={@endpoint.labels} label={:labels} value={@endpoint.labels} />
    </section>
    """
  end

  attr :endpoint, :map, required: true
  attr :form, :map, required: true
  attr :declared_params, :list, default: []
  attr :result, :map, default: nil

  def endpoint_test(assigns) do
    ~H"""
    <aside id="endpoint-test" class="tw-flex tw-min-w-0 tw-flex-col tw-gap-8">
      <div>
        <h3 class="tw-mb-2 tw-text-xl tw-font-semibold tw-text-white">Test your endpoint</h3>
        <p class="tw-mb-5 tw-text-sm tw-text-zinc-400">
          Enter parameter values and optionally add a consumer query before running the endpoint.
        </p>

        <section class="tw-rounded tw-border tw-bg-dashboard-grey tw-p-5">
          <.form :let={f} for={@form} phx-submit="run-query" aria-label="Test endpoint">
            {hidden_input(f, :query)}

            <fieldset class="tw-mb-5">
              <legend class="tw-mb-3 tw-text-base tw-font-semibold tw-text-white">Parameters</legend>
              <p :if={@declared_params == []} class="tw-mb-0 tw-text-sm tw-text-zinc-500">
                This endpoint has no parameters.
              </p>
              <.inputs_for :let={params_f} field={f[:params]}>
                <div :for={key <- @declared_params} class="tw-mb-3">
                  {label(params_f, key, key, class: "tw-mb-1 tw-block tw-text-sm tw-text-white")}
                  {text_input(params_f, key, class: "form-control tw-w-full")}
                </div>
              </.inputs_for>

              <div :if={@endpoint.enable_dynamic_reservation} class="form-group tw-mb-0 tw-mt-3">
                {label(f, :reservation, "BigQuery Reservation")}
                {text_input(f, :reservation,
                  class: "form-control",
                  placeholder: "projects/{project}/locations/{location}/reservations/{reservation}"
                )}
              </div>
            </fieldset>

            <fieldset :if={@endpoint.sandboxable} class="tw-mb-5 tw-border-0 tw-border-t tw-border-solid tw-border-zinc-700 tw-p-0 tw-pt-5">
              <legend class="tw-mb-2 tw-text-base tw-font-semibold tw-text-white">
                Consumer query <span class="tw-font-normal tw-text-zinc-500">(optional)</span>
              </legend>
              <p class="tw-mb-4 tw-text-sm tw-text-zinc-400">
                Test how consumers can query your endpoint using the <code class="tw-whitespace-nowrap tw-rounded tw-bg-zinc-700 tw-px-1 tw-text-xs">?sql=</code> or <code class="tw-whitespace-nowrap tw-rounded tw-bg-zinc-700 tw-px-1 tw-text-xs">?lql=</code> parameter.
                Queries are restricted to the CTE tables defined in your endpoint query.
              </p>

              <div class="tw-mb-3 tw-flex tw-gap-4">
                <label class="tw-flex tw-items-center tw-gap-1 tw-text-sm tw-text-white">
                  {radio_button(f, :query_mode, "sql")} SQL
                </label>
                <label class="tw-flex tw-items-center tw-gap-1 tw-text-sm tw-text-white">
                  {radio_button(f, :query_mode, "lql")} LQL
                </label>
              </div>

              {textarea(f, :consumer_query,
                placeholder: "SELECT * FROM cte_name WHERE condition...",
                rows: 6,
                class: "form-control tw-w-full tw-rounded tw-border-zinc-700 tw-bg-zinc-900 tw-p-2 tw-font-mono tw-text-sm tw-text-white"
              )}

              <label class="tw-mt-3 tw-flex tw-items-center tw-text-sm tw-text-white">
                {checkbox(f, :show_transformed, class: "tw-mr-2")} Show transformed query
              </label>
            </fieldset>

            {submit("Test endpoint", class: "btn btn-secondary")}
          </.form>

          <.test_result result={@result} />
        </section>
      </div>

      <.endpoint_call_examples endpoint={@endpoint} declared_params={@declared_params} />
    </aside>
    """
  end

  attr :endpoint, :map, required: true

  def endpoint_query_panel(assigns) do
    ~H"""
    <.query_panel language={@endpoint.language} query={@endpoint.query} />
    """
  end

  attr :endpoint, :map, required: true
  attr :parsed_result, :map, default: nil
  attr :redact_pii, :boolean, default: false

  defp endpoint_query_tabs(assigns) do
    ~H"""
    <section>
      <ul class="nav d-flex tw-mb-3" id="endpoint-query-nav" role="tablist">
        <li class="nav-item">
          <a class="nav-link active tw-rounded [&.active]:tw-bg-dashboard-grey [&.active]:tw-text-white" id="endpoint-query-link" data-toggle="tab" href="#endpoint-query" role="tab" aria-controls="endpoint-query" aria-selected="true">
            Query
          </a>
        </li>
        <li :if={@parsed_result} class="nav-item">
          <a class="nav-link tw-rounded [&.active]:tw-bg-dashboard-grey [&.active]:tw-text-white" id="expanded-endpoint-query-link" data-toggle="tab" href="#expanded-endpoint-query" role="tab" aria-controls="expanded-endpoint-query" aria-selected="false">
            Expanded query
          </a>
        </li>
      </ul>

      <div class="tab-content" id="endpoint-query-tabs">
        <div class="tab-pane active" id="endpoint-query" role="tabpanel" aria-labelledby="endpoint-query-link">
          <.endpoint_query_panel endpoint={@endpoint} />
        </div>
        <div :if={@parsed_result} class="tab-pane" id="expanded-endpoint-query" role="tabpanel" aria-labelledby="expanded-endpoint-query-link">
          <.query_panel language={@endpoint.language} query={maybe_redact_query(@parsed_result.expanded_query, @redact_pii)} />
        </div>
      </div>
    </section>
    """
  end

  attr :language, :atom, required: true
  attr :query, :string, required: true

  defp query_panel(assigns) do
    ~H"""
    <section class="tw-rounded tw-bg-dashboard-grey tw-p-4">
      <div class="tw-mb-2 tw-font-bold tw-text-sm tw-text-zinc-500">
        {format_language(@language)}
      </div>

      <div :if={not sql_query_language?(@language)} class="tw-rounded-md tw-p-4">
        <pre class="tw-m-0 tw-whitespace-pre-wrap tw-break-words tw-font-mono tw-text-sm tw-text-zinc-200"><code>{@query}</code></pre>
      </div>

      <div :if={sql_query_language?(@language)} class="tw-rounded-md tw-p-4">
        <QueryComponents.formatted_sql sql_string={@query} />
      </div>
    </section>
    """
  end

  attr :label, :any, required: true
  attr :value, :string, required: true

  defp setting_row(assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-2 tw-gap-4 tw-py-4">
      <div class="tw-text-sm tw-font-semibold tw-text-zinc-100">{format_label(@label)}:</div>
      <div class="tw-text-sm tw-text-zinc-300">{@value}</div>
    </div>
    """
  end

  attr :label, :any, required: true
  attr :value, :string, required: true

  defp property_row(assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-[12rem_minmax(0,1fr)] tw-gap-4 tw-border-0 tw-border-b tw-border-solid tw-border-zinc-800 tw-py-4 last:tw-border-b-0">
      <div class="tw-text-sm tw-font-semibold tw-text-zinc-100">{format_label(@label)}:</div>
      <div class="tw-min-w-0 tw-break-words tw-text-sm tw-text-zinc-300">{@value}</div>
    </div>
    """
  end

  attr :endpoint, :map, required: true
  attr :declared_params, :list, default: []

  defp endpoint_call_examples(assigns) do
    ~H"""
    <section id="endpoint-call-examples">
      <h3 class="tw-mb-3 tw-text-xl tw-font-semibold tw-text-white">Call your endpoint</h3>
      <div class="tw-flex tw-flex-col tw-gap-3">
        <.curl_example title="By UUID" identifier={@endpoint.token} declared_params={@declared_params} />
        <.curl_example :if={@endpoint.enable_auth} title="By name" identifier={@endpoint.name} declared_params={@declared_params} />
        <.curl_example
          title="With per-request PII redaction"
          identifier={@endpoint.token}
          headers={[
            "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
            "-H 'LF-ENDPOINT-REDACT-PII: true'",
            "-H 'Content-Type: application/json; charset=utf-8'"
          ]}
          declared_params={@declared_params}
        />
        <.curl_example
          :if={@endpoint.enable_dynamic_reservation}
          title="With dynamic BigQuery reservation"
          identifier={@endpoint.token}
          headers={[
            "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
            "-H 'LF-ENDPOINT-BIGQUERY-RESERVATION: projects/PROJECT/locations/LOCATION/reservations/RESERVATION'",
            "-H 'Content-Type: application/json; charset=utf-8'"
          ]}
        />
      </div>
    </section>
    """
  end

  attr :title, :string, required: true
  attr :identifier, :any, required: true

  attr :headers, :list,
    default: [
      "-H 'X-API-KEY: YOUR-ACCESS-TOKEN'",
      "-H 'Content-Type: application/json; charset=utf-8'"
    ]

  attr :declared_params, :list, default: []

  defp curl_example(assigns) do
    assigns =
      assign(
        assigns,
        :command,
        curl_command(assigns.identifier, assigns.headers, assigns.declared_params)
      )

    ~H"""
    <div class="tw-rounded tw-bg-zinc-800 tw-p-4">
      <div class="tw-mb-2 tw-flex tw-items-center tw-justify-between tw-gap-3">
        <h4 class="tw-mb-0 tw-text-sm tw-font-semibold tw-text-zinc-300">{@title}</h4>
        <.clipboard_button text={@command} label="" class="btn-sm tw-shrink-0" title={"Copy #{@title} request"} aria-label={"Copy #{@title} request"} />
      </div>
      <pre class="tw-mb-0 tw-overflow-x-auto tw-whitespace-pre-wrap tw-break-words"><code class="tw-text-xs tw-text-zinc-200">{@command}</code></pre>
    </div>
    """
  end

  attr :result, :map, default: nil

  defp test_result(assigns) do
    ~H"""
    <div :if={@result}>
      <div :if={@result.kind == :endpoint and @result.status == :error} class="tw-mt-4">
        <.alert variant="danger">
          <strong>Query error!</strong>
          <br />
          <span>{@result.error}</span>
        </.alert>
      </div>

      <div :if={@result.kind == :consumer and @result.status == :error} class="tw-mt-5">
        <.alert variant="danger">
          {@result.error}
        </.alert>
      </div>

      <div :if={@result.status == :ok} class="tw-mt-5">
        <div class="tw-mb-2 tw-flex tw-justify-between">
          <h5 class="tw-text-white">
            {if @result.kind == :consumer, do: "Consumer Query Results", else: "Results"}
          </h5>
          <QueryComponents.query_cost :if={is_integer(@result.total_bytes_processed)} bytes={@result.total_bytes_processed} />
        </div>
        <code class="tw-block tw-overflow-x-auto tw-whitespace-pre-wrap tw-rounded tw-bg-zinc-800 tw-p-2 tw-text-xs tw-text-white">
          {Jason.encode!(@result.rows) |> Jason.Formatter.pretty_print()}
        </code>
        <details :if={@result.transformed_query} class="tw-mt-4">
          <summary class="tw-cursor-pointer tw-text-sm tw-font-semibold tw-text-white">
            Show Transformed Query
          </summary>
          <code class="tw-mt-2 tw-block tw-whitespace-pre-wrap tw-rounded tw-bg-zinc-900 tw-p-2 tw-text-xs tw-text-white">
            {@result.transformed_query}
          </code>
        </details>
      </div>
    </div>
    """
  end

  attr :change, :map, required: true

  def change(%{change: %{query_diff: _}} = assigns) do
    ~H"""
    <div class="tw-grid tw-w-full tw-grid-cols-[12rem_minmax(0,1fr)] tw-items-start tw-gap-2 tw-rounded-sm tw-px-2 tw-py-1 tw-text-sm tw-text-zinc-300">
      <span class="tw-pr-2 tw-font-medium tw-text-zinc-300">Query:</span>
      <div class="tw-min-w-0 tw-rounded-sm tw-px-2 tw-py-1 tw-font-mono tw-text-xs tw-text-zinc-200 [&_pre]:tw-m-0 [&_pre]:tw-whitespace-pre-wrap [&_pre]:tw-break-words [&_pre]:tw-overflow-x-visible">
        <pre class=""><%= for segment <- @change.query_diff do %><span class={segment.class}>{segment.value}</span><% end %></pre>
      </div>
    </div>
    """
  end

  def change(assigns) do
    assigns = assign(assigns, :value, format_change_value(assigns.change))

    ~H"""
    <div class="tw-grid tw-w-full tw-grid-cols-[12rem_minmax(0,1fr)] tw-items-baseline tw-gap-2 tw-rounded-sm tw-px-2 tw-py-1 tw-text-sm tw-text-zinc-300">
      <span class="tw-pr-2 tw-font-medium tw-text-zinc-300">{format_label(@change.field)}:</span>
      <span class="tw-min-w-0 tw-text-zinc-400 tw-break-words">
        {@value}
      </span>
    </div>
    """
  end

  defp curl_command(identifier, headers, declared_params) do
    identifier = URI.encode(to_string(identifier), &URI.char_unreserved?/1)
    url = "https://api.logflare.app/api/endpoints/query/#{identifier}"

    parameter_arguments =
      case declared_params do
        [] -> []
        params -> ["-G " <> Enum.map_join(params, " ", fn param -> ~s(-d "#{param}=VALUE") end)]
      end

    separator = " " <> "\\" <> "\n  "
    Enum.join([~s(curl "#{url}") | headers] ++ parameter_arguments, separator)
  end

  defp maybe_redact_query(query, redact_pii) when is_binary(query) do
    if redact_pii do
      PiiRedactor.redact_pii_from_value(query)
    else
      query
    end
  end

  defp maybe_redact_query(query, _redact_pii), do: query

  defp format_change_value(%{value: value}) when is_boolean(value), do: format_toggle(value)

  defp format_change_value(%{field: field, value: value})
       when field in ["cache_duration_seconds", "proactive_requerying_seconds"],
       do: format_cache_duration(value)

  defp format_change_value(%{field: "source_mapping", value: value}) when is_map(value),
    do: value |> Map.keys() |> Enum.join("")

  defp format_change_value(%{field: "source_mapping", value: value}), do: inspect(value)
  defp format_change_value(%{value: nil}), do: "—"
  defp format_change_value(%{value: value}), do: to_string(value)

  defp format_language(:bq_sql), do: "BigQuery SQL"
  defp format_language(:ch_sql), do: "ClickHouse SQL"
  defp format_language(:pg_sql), do: "Postgres SQL"
  defp format_language(:lql), do: "Logflare Query Language"
  defp format_language(language), do: language |> to_string() |> Phoenix.Naming.humanize()

  defp format_cache_duration(n) when n in [0, nil], do: "disabled"
  defp format_cache_duration(value), do: to_string(value) <> " seconds"

  defp format_toggle(true), do: "enabled"
  defp format_toggle(value) when value in [false, nil], do: "disabled"
  defp format_toggle(other), do: inspect(other)

  defp format_label(field) when is_binary(field),
    do: field |> String.to_existing_atom() |> format_label()

  defp format_label(field), do: @field_labels[field] || Phoenix.Naming.humanize(field)

  defp sql_query_language?(language), do: language in [:bq_sql, :ch_sql, :pg_sql]
end
