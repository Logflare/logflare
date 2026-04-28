defmodule LogflareWeb.Endpoints.Components do
  use LogflareWeb, :html
  use Phoenix.Component

  alias LogflareWeb.QueryComponents

  @field_labels [
    enable_auth: "Authentication",
    max_limit: "Max rows",
    cache_duration_seconds: "Caching",
    proactive_requerying_seconds: "Cache warming",
    sandboxable: "Query sandboxing",
    redact_pii: "Redact PII",
    enable_dynamic_reservation: "Dynamic reservation"
  ]

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

  def endpoint_query_panel(assigns) do
    ~H"""
    <section class="tw-rounded tw-bg-[#1d1d1d] tw-p-4">
      <div class="tw-mb-2 tw-font-bold tw-text-sm tw-text-zinc-500">
        {format_language(@endpoint.language)}
      </div>

      <div :if={not sql_query?(@endpoint)} class="tw-rounded-md tw-p-4">
        <pre class="tw-m-0 tw-whitespace-pre-wrap tw-break-words tw-font-mono tw-text-sm tw-text-zinc-200"><code>{@endpoint.query}</code></pre>
      </div>

      <div :if={sql_query?(@endpoint)} class="tw-rounded-md tw-p-4">
        <QueryComponents.formatted_sql sql_string={@endpoint.query} />
      </div>
    </section>
    """
  end

  attr :label, :any, required: true
  attr :value, :string, required: true

  defp setting_row(assigns) do
    ~H"""
    <div class="tw-grid tw-grid-cols-2 tw-gap-4 tw-py-4 ">
      <div class="tw-text-sm tw-font-semibold tw-text-zinc-100">{format_label(@label)}:</div>
      <div class="tw-text-sm tw-text-zinc-300">{@value}</div>
    </div>
    """
  end

  attr :change, :map, required: true
  attr :field, :string
  attr :value, :string

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

  def change(%{value: _value} = assigns) do
    ~H"""
    <div class="tw-grid tw-w-full tw-grid-cols-[12rem_minmax(0,1fr)] tw-items-baseline tw-gap-2 tw-rounded-sm tw-px-2 tw-py-1 tw-text-sm tw-text-zinc-300">
      <span class="tw-pr-2 tw-font-medium tw-text-zinc-300">{format_label(@change.field)}:</span>
      <span class="tw-min-w-0 tw-text-zinc-400 tw-break-words">
        {@value}
      </span>
    </div>
    """
  end

  def change(%{change: %{value: value}} = assigns) when is_boolean(value),
    do:
      assigns
      |> assign(value: format_toggle(value))
      |> change()

  def change(%{change: %{field: field, value: value}} = assigns)
      when field in ["cache_duration_seconds", "proactive_requerying_seconds"],
      do: assigns |> assign(value: format_cache_duration(value)) |> change()

  def change(%{change: %{field: "source_mapping"}} = assigns) do
    value =
      case assigns.change.value do
        source when is_map(source) -> source |> Map.keys() |> Enum.join("")
        other -> inspect(other)
      end

    assigns |> assign(value: value) |> change()
  end

  def change(%{change: change} = assigns) do
    assigns
    |> assign(:value, if(change.value == nil, do: "—", else: to_string(change.value)))
    |> change()
  end

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

  defp sql_query?(%{language: language}), do: language in [:bq_sql, :ch_sql, :pg_sql]
end
