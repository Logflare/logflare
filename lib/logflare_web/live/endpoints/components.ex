defmodule LogflareWeb.Endpoints.Components do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Logflare.Sql
  alias LogflareWeb.Endpoints.RunQuery
  alias LogflareWeb.QueryComponents

  embed_templates("components/*")

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

  def docs_link(assigns) do
    ~H"""
    <.subheader_link to="https://docs.logflare.app/concepts/endpoints" external={true} text="docs" fa_icon="book" />
    """
  end

  attr :team, :any, default: nil

  def access_tokens_link(assigns) do
    ~H"""
    <.subheader_link team={@team} to="/access-tokens" text="access tokens" fa_icon="key" />
    """
  end

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

  def endpoint_details(assigns) do
    ~H"""
    <div id="endpoint-details" class="tw-flex tw-min-w-0 tw-flex-col tw-gap-8">
      <p class="tw-mb-0 tw-max-w-[65ch] tw-whitespace-pre-wrap tw-text-lg tw-leading-relaxed tw-text-zinc-200">{@endpoint.description || "—"}</p>

      <.endpoint_query_tabs endpoint={@endpoint} parsed_result={@parsed_result} />

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
      <.property_row label={:token} value={@endpoint.token} copyable />
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
  attr :parsed_result, :map, default: nil

  defp endpoint_query_tabs(assigns) do
    assigns =
      assign(assigns, :expanded_query, Map.get(assigns.parsed_result || %{}, :expanded_query))

    ~H"""
    <section>
      <ul class="nav d-flex tw-mb-3" id="endpoint-query-nav" role="tablist">
        <li class="nav-item">
          <a class="nav-link active tw-rounded [&.active]:tw-bg-dashboard-grey [&.active]:tw-text-white" id="endpoint-query-link" data-toggle="tab" href="#endpoint-query" role="tab" aria-controls="endpoint-query" aria-selected="true">
            Query
          </a>
        </li>
        <li :if={show_expanded_query?(@endpoint, @expanded_query)} class="nav-item">
          <a class="nav-link tw-rounded [&.active]:tw-bg-dashboard-grey [&.active]:tw-text-white" id="expanded-endpoint-query-link" data-toggle="tab" href="#expanded-endpoint-query" role="tab" aria-controls="expanded-endpoint-query" aria-selected="false">
            Expanded query
          </a>
        </li>
      </ul>

      <div class="tab-content" id="endpoint-query-tabs">
        <div class="tab-pane active" id="endpoint-query" role="tabpanel" aria-labelledby="endpoint-query-link">
          <.query_panel language={@endpoint.language} query={@endpoint.query} />
        </div>
        <div :if={show_expanded_query?(@endpoint, @expanded_query)} class="tab-pane" id="expanded-endpoint-query" role="tabpanel" aria-labelledby="expanded-endpoint-query-link">
          <.query_panel language={@endpoint.language} query={@expanded_query} />
        </div>
      </div>
    </section>
    """
  end

  attr :language, :atom, required: true
  attr :query, :string, required: true

  def query_panel(assigns) do
    ~H"""
    <section class="tw-rounded tw-bg-dashboard-grey tw-p-4">
      <div class="tw-mb-2 tw-font-bold tw-text-sm tw-text-zinc-500">
        {format_language(@language)}
      </div>

      <div class="tw-rounded-md tw-p-4">
        <pre :if={not sql_query_language?(@language)} class="tw-m-0 tw-whitespace-pre-wrap tw-break-words tw-font-mono tw-text-sm tw-text-zinc-200"><code>{@query}</code></pre>
        <QueryComponents.formatted_sql :if={sql_query_language?(@language)} sql_string={@query} />
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
  attr :copyable, :boolean, default: false

  defp property_row(assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-[12rem_minmax(0,1fr)] tw-gap-4 tw-border-0 tw-border-b tw-border-solid tw-border-zinc-800 tw-py-4 last:tw-border-b-0">
      <div class="tw-text-sm tw-font-semibold tw-text-zinc-100">{format_label(@label)}:</div>
      <div class="tw-group tw-flex tw-min-w-0 tw-items-start tw-gap-2">
        <span class="tw-min-w-0 tw-break-words tw-text-sm tw-text-zinc-300">{@value}</span>
        <.clipboard_button :if={@copyable} text={@value} label="" class="btn-sm tw-ml-auto tw-shrink-0 tw-invisible group-hover:tw-visible group-focus-within:tw-visible" title={"Copy #{format_label(@label)}"} aria-label={"Copy #{format_label(@label)}"} />
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

  defp show_expanded_query?(endpoint, expanded_query) when is_binary(expanded_query) do
    with {:ok, formatted_expanded_query} <- Sql.format(expanded_query),
         {:ok, formatted_query} <- Sql.format(endpoint.query) do
      formatted_expanded_query != formatted_query
    else
      _ -> expanded_query != endpoint.query
    end
  end

  defp show_expanded_query?(_endpoint, _expanded_query), do: false

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
