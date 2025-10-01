defmodule LogflareWeb.DashboardLive.DashboardSourceComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Logflare.Sources.Source
  alias Phoenix.LiveView.JS

  attr :source, Source, required: true
  attr :metrics, Source.Metrics, required: true
  attr :plan, :map, required: true
  attr :fade_in, :boolean, default: false

  def source_item(assigns) do
    ~H"""
    <li class="list-group-item" id={"source-#{@source.token}"}>
      <div class="favorite float-left tw-cursor-pointer tw-text-yellow-200" phx-click="toggle_favorite" phx-value-favorite={!@source.favorite} phx-value-id={@source.id}>
        <span>
          <i class={[if(@source.favorite, do: "fas", else: "far"), "fa-star "]} />
        </span>
      </div>
      <div>
        <div class="float-right">
          <.link href={~p"/sources/#{@source}/edit"} class="dashboard-links">
            <i class="fas fa-edit"></i>
          </.link>
        </div>
        <div class="source-link word-break-all">
          <.link href={~p"/sources/#{@source}"} class="tw-text-white"><%= @source.name %></.link>
          <span>
            <.inserts_badge count={@metrics.inserts} source_token={@source.token} fade_in={@fade_in} />
          </span>
        </div>
      </div>
      <.source_metadata source={@source} metrics={@metrics} plan={@plan} />
    </li>
    """
  end

  attr :source, Logflare.Sources.Source, required: true
  attr :metrics, Logflare.Sources.Source.Metrics, required: true
  attr :plan, :map, required: true
  attr :fade_in, :boolean, default: false

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
        <.source_metrics source={@source} metrics={@metrics} rate_limit={@plan.limit_source_rate_limit} fields_limit={@plan.limit_source_fields_limit} />
      </div>
    </div>
    """
  end

  attr :source, Logflare.Sources.Source, required: true
  attr :metrics, Logflare.Sources.Source.Metrics, required: true
  attr :rate_limit, :integer, required: true
  attr :fields_limit, :integer, required: true

  def source_metrics(assigns) do
    ~H"""
    <div>
      <.metric>
        latest: <span :if={@metrics.latest == 0}>not initialized</span>
        <span :if={@metrics.latest != 0} class="log-datestamp" id={metric_id(@source, "latest")} data-timestamp={@metrics.latest}>
          <span xclass="tw-sr-only tw-inline-block tw-invisible tw-w-24"><%= @metrics.latest %></span>
        </span>
      </.metric>

      <.metric>
        rate: <span id={metric_id(@source, "rate")}><%= @metrics.rate %>/s</span>
      </.metric>

      <.metric>
        avg:
        <.tooltip :if={rate_limit_warning?(@source, @rate_limit)} id={metric_id(@source, "avg-rate")} class="my-badge my-badge-warning" placement="left" title={"Source rate limit is avg #{@rate_limit} events/sec! Upgrade for more."}>
          <%= @metrics.avg %>
        </.tooltip>
        <span :if={not rate_limit_warning?(@source, @rate_limit)} id={metric_id(@source, "avg-rate")}><%= @metrics.avg %></span>
      </.metric>

      <.metric>
        max: <span id={metric_id(@source, "max-rate")}><%= @metrics.max %></span>
      </.metric>

      <.metric>
        buffer:
        <.tooltip title={"Pipelines #{pipeline_count(@source)}"} id={metric_id(@source, "buffer")}>
          <%= @metrics.buffer %>
        </.tooltip>
      </.metric>

      <.metric>
        fields:
        <.tooltip :if={fields_limit_warning?(@source, @fields_limit)} placement="left" class="my-badge my-badge-warning" title={"Max #{@fields_limit} fields per source! Data in new fields are ignored. Upgrade for more."}>
          <%= @metrics.fields %>
        </.tooltip>
        <span :if={not fields_limit_warning?(@source, @fields_limit)}><%= @metrics.fields %></span>
      </.metric>

      <.metric>
        rejected:
        <.link :if={@metrics.rejected > 0} href={~p"/sources/#{@source}/rejected"}>
          <.tooltip class="my-badge my-badge-warning" placement="left" title="Some events didn't validate!"><%= @metrics.rejected %></.tooltip>
        </.link>
        <span :if={@metrics.rejected == 0} id={metric_id(@source, "rejected")}><%= @metrics.rejected %></span>
      </.metric>

      <.metric>
        ttl: <%= @source.retention_days %> <%= if(@source.retention_days == 1, do: "day", else: "days") %>
      </.metric>
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

  attr :source_token, :any
  attr :count, :string
  attr :fade_in, :boolean, default: false

  def inserts_badge(assigns) do
    assigns = assigns |> assign(:id, "source-#{assigns.source_token}-inserts-#{assigns.count}")

    ~H"""
    <small class="my-badge my-badge-info tw-transition-colors tw-ease-in" id={@id} phx-mounted={if(@fade_in, do: JS.transition("tw-bg-blue-500", time: 500))}>
      <%= Number.Delimit.number_to_delimited(@count) %>
    </small>
    """
  end

  attr :title, :string, required: true
  attr :id, :string, required: false
  attr :placement, :string, default: "top"
  attr :class, :string, default: ""
  attr :rest, :global
  slot :inner_block

  def tooltip(assigns) do
    ~H"""
    <span class={["logflare-tooltip", @class]} id={@id} data-placement={@placement} title={@title} data-toggle="tooltip">
      <%= render_slot(@inner_block) %>
    </span>
    """
  end

  @spec pipeline_count(Source.t()) :: non_neg_integer()
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
