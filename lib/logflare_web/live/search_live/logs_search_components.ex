defmodule LogflareWeb.SearchLive.LogsSearchComponents do
  @moduledoc """
  Components for logs search page layout and controls.
  """
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  alias Logflare.Lql.Rules
  alias LogflareWeb.SearchLive.LogsSearchButtonComponents

  attr :form, Phoenix.HTML.Form, required: true
  attr :querystring, :string, required: true
  attr :search_history, :list, required: true
  attr :search_timezone, :string, required: true

  def search_form(assigns) do
    ~H"""
    <div class="form-group form-text">
      {text_input(@form, :querystring,
        phx_focus: :form_focus,
        phx_blur: :form_blur,
        value: @querystring,
        class: "form-control form-control-margin",
        list: "matches"
      )}
      {text_input(@form, :search_timezone,
        class: "d-none",
        value: @search_timezone,
        id: "search-timezone"
      )}
      <datalist id="matches">
        <%= for s <- @search_history do %>
          <option value={s.querystring}>{s.querystring}</option>
        <% end %>
      </datalist>
    </div>
    """
  end

  attr :form, Phoenix.HTML.Form, required: true
  attr :lql_rules, :list, required: true
  attr :chart_aggregate_enabled?, :boolean, required: true

  def chart_controls(assigns) do
    ~H"""
    <div class="d-flex flex-wrap align-items-center form-text pt-2">
      <div class="pr-3 pt-1 pb-1  hide-on-mobile">
        Chart period:
      </div>
      <div class="pr-3 pt-1 pb-1">
        {select(@form, :chart_period, ["day", "hour", "minute", "second"],
          selected: Rules.get_chart_period(@lql_rules, "minute"),
          class: "form-control form-control-sm"
        )}
      </div>
      <div class="pr-3 pt-1 pb-1 hide-on-mobile">
        Aggregate:
      </div>
      <div class="pr-3 pt-1 pb-1">
        <%= if @chart_aggregate_enabled? do %>
          {select(
            @form,
            :chart_aggregate,
            ["sum", "avg", "max", "p50", "p95", "p99", "count"],
            selected: Rules.get_chart_aggregate(@lql_rules, "count"),
            class: "form-control form-control-sm"
          )}
        <% else %>
          <span class="d-inline-block" tabindex="0" data-toggle="tooltip" title="Chart aggregate setting requires usage of chart: operator" trigger="hover click" delay="0">
            {select(
              @form,
              :chart_aggregate,
              ["count"],
              selected: "count",
              class: "form-control form-control-sm",
              style: "pointer-events: none;"
            )}
          </span>
        <% end %>
      </div>
    </div>
    """
  end

  attr :last_query_completed_at, :any, default: nil

  def query_timing(assigns) do
    assigns =
      assigns
      |> assign_new(:timestamp, fn ->
        if assigns.last_query_completed_at,
          do: Timex.to_unix(assigns.last_query_completed_at),
          else: false
      end)

    ~H"""
    <small class="form-text text-muted" id="last-query-completed-at" data-timestamp={@timestamp}>
      Elapsed since last query: <span id="elapsed" phx-update="ignore"> 0.0 </span> seconds
    </small>
    """
  end

  attr :search_form, :any, required: true
  attr :querystring, :string, required: true
  attr :search_history, :list, required: true
  attr :search_timezone, :string, required: true
  attr :loading, :boolean, required: true
  attr :tailing?, :boolean, required: true
  attr :uri_params, :map, required: true
  attr :lql_rules, :list, required: true
  attr :user, Logflare.User, required: true
  attr :search_op_log_events, :any, default: nil
  attr :search_op_log_aggregates, :any, default: nil
  attr :has_results?, :boolean
  attr :source, Logflare.Sources.Source, required: true
  attr :last_query_completed_at, :any, default: nil

  def search_controls(assigns) do
    ~H"""
    <div class="search-control" id="source-logs-search-control" phx-hook="SourceLogsSearch">
      <.form :let={f} for={@search_form} action="#" phx-submit="start_search" phx-change="form_update" class="form-group">
        <.search_form form={f} querystring={@querystring} search_history={@search_history} search_timezone={@search_timezone} />

        <div class="d-flex flex-wrap align-items-center form-text">
          <div class="pr-2 pt-1 pb-1">
            <%= submit disabled: @loading, id: "search", class: "btn btn-primary" do %>
              <i class="fas fa-search"></i><span class="fas-in-button hide-on-mobile">Search</span>
            <% end %>
          </div>

          <LogsSearchButtonComponents.navigation_buttons tailing?={@tailing?} uri_params={@uri_params} />

          <LogsSearchButtonComponents.action_buttons source={@source} user={@user} has_results?={@has_results?} />
        </div>

        <.chart_controls form={f} lql_rules={@lql_rules} chart_aggregate_enabled?={search_agg_controls_enabled?(@lql_rules)} />

        <div id="observer-target"></div>
      </.form>

      <.query_timing last_query_completed_at={@last_query_completed_at} />
    </div>
    """
  end

  defp search_agg_controls_enabled?(lql_rules) do
    lql_rules
    |> Enum.find(%{}, &match?(%Logflare.Lql.Rules.ChartRule{}, &1))
    |> Map.get(:value_type)
    |> Kernel.in([:integer, :float])
  end
end
