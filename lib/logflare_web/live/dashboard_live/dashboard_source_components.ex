defmodule LogflareWeb.DashboardLive.DashboardSourceComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  def source_metadata(assigns) do
    ~H"""
    <div class="tw-ml-8">
      <div>
        <small class="source-details">
          id:
          <span
            class="pointer-cursor copy-token logflare-tooltip copy-tooltip"
            phx-click={Phoenix.LiveView.JS.dispatch("logflare:copy-to-clipboard", detail: %{text: @source.token})}
            data-toggle="tooltip"
            data-placement="top"
            title="Copy this"
            id={String.replace(Atom.to_string(@source.token), ~r/[0-9]|-/, "")}
          >
            <%= @source.token %>
          </span>
        </small>
      </div>
      <div>
        <.source_metrics source={@source} rate_limit={@plan.limit_source_rate_limit} fields_limit={@plan.limit_source_fields_limit} />
      </div>
    </div>
    """
  end

  slot :inner_block

  def metric(assigns) do
    ~H"""
    <div class="tw-mr-3 tw-inline-block tw-text-sm">
      <%= render_slot(@inner_block) %>
    </div>
    """
  end

  attr :source, Logflare.Sources.Source, required: true
  attr :rate_limit, :integer, required: true
  attr :fields_limit, :integer, required: true

  def source_metrics(assigns) do
    ~H"""
    <div>
      <.metric>
        latest: <span :if={@source.metrics.latest == 0}>not initialized</span>
        <span :if={@source.metrics.latest != 0} class="log-datestamp" id={metric_id(@source, "latest")} data-timestamp={@source.metrics.latest}>
          <span xclass="tw-sr-only tw-inline-block tw-invisible tw-w-24"><%= @source.metrics.latest %></span>
        </span>
      </.metric>

      <.metric>
        rate: <span id={metric_id(@source, "rate")}><%= @source.metrics.rate %>/s</span>
      </.metric>

      <.metric>
        avg:
        <.tooltip :if={rate_limit_warning?(@source, @rate_limit)} id={metric_id(@source, "avg-rate")} class="my-badge my-badge-warning" placement="left" title={"Source rate limit is avg #{@limit_source_rate_limit} events/sec! Upgrade for more."}>
          <%= @source.metrics.avg %>
        </.tooltip>
        <span :if={not rate_limit_warning?(@source, 100)} id={metric_id(@source, "avg-rate")}><%= @source.metrics.avg %></span>
      </.metric>

      <.metric>
        max: <span id={metric_id(@source, "max-rate")}><%= @source.metrics.max %></span>
      </.metric>

      <.metric>
        buffer:
        <.tooltip title={"Pipelines #{pipeline_count(@source)}"} id={metric_id(@source, "buffer")}>
          <%= @source.metrics.buffer %>
        </.tooltip>
      </.metric>

      <.metric>
        fields:
        <.tooltip :if={fields_limit_warning?(@source, @fields_limit)} placement="left" class="my-badge my-badge-warning" title={"Max #{@limit_source_fields_limit} fields per source! Data in new fields are ignored. Upgrade for more."}>
          <%= @source.metrics.fields %>
        </.tooltip>
        <span :if={not fields_limit_warning?(@source, @fields_limit)}><%= @source.metrics.fields %></span>
      </.metric>

      <.metric>
        rejected:
        <.link :if={@source.metrics.rejected > 0} href={~p"/source/#{@source}/rejected"}>
          <.tooltip class="my-badge my-badge-warning" placement="left" title="Some events didn't validate!"><%= @source.metrics.rejected %></.tooltip>
        </.link>
        <span :if={@source.metrics.rejected == 0} id={metric_id(@source, "rejected")}><%= @source.metrics.rejected %></span>
      </.metric>

      <.metric>
        ttl: <%= @source.retention_days %> <%= if(@source.retention_days == 1, do: "day", else: "days") %>
      </.metric>
    </div>
    """
  end

  attr :title, :string, required: true
  attr :placement, :string, default: "top"
  attr :class, :string, default: ""
  attr :rest, :global
  slot :inner_block

  def tooltip(assigns) do
    ~H"""
    <span class={["logflare-tooltip", @class]} data-placement={@placement} title={@title} data-toggle="tooltip">
      <%= render_slot(@inner_block) %>
    </span>
    """
  end

  def pipeline_count(source) do
    name = Logflare.Backends.via_source(source.id, Logflare.Sources.Source.BigQuery.Pipeline, nil)

    if GenServer.whereis(name) do
      Logflare.Backends.DynamicPipeline.pipeline_count(name)
    else
      0
    end
  end

  defp metric_id(source, key), do: [to_string(source.token), "-", key]

  defp rate_limit_warning?(source, limit), do: source.metrics.avg >= 0.80 * limit

  defp fields_limit_warning?(source, limit), do: source.metrics.fields > limit
end
