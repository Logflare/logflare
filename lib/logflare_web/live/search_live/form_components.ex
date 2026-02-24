defmodule LogflareWeb.SearchLive.FormComponents do
  @moduledoc """
  Components for logs search page layout and controls.
  """
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  alias Logflare.Lql.Rules
  alias Logflare.Utils
  alias Logflare.Sources.Source

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

  attr :fields, :list, required: true
  attr :id_prefix, :string, default: "recommended-field"

  def recommended_field_inputs(assigns) do
    assigns =
      assigns
      |> assign(:fields, format_recommended_fields(assigns.fields))

    ~H"""
    <div :if={Enum.any?(@fields)} class="form-text" id="recommended_fields" phx-update="ignore">
      <div class="d-flex flex-wrap">
        <div :for={field <- @fields} class="pr-2 pt-1 pb-1">
          <div class="tw-flex tw-justify-between tw-items-baseline">
            <label for={"#{@id_prefix}-#{field.name}"} class="tw-mb-0 tw-text-xs tw-text-gray-300 tw-block">{field.name}</label>
            <span :if={field.required?} class="required-field-indicator tw-text-gray-500 tw-block tw-text-right tw-text-xs">
              required
            </span>
          </div>
          <input id={"#{@id_prefix}-#{field.name}"} name={input_name(:fields, field.name)} class="form-control form-control-sm tw-text-xs " type="text" />
        </div>
      </div>
    </div>
    """
  end

  defp format_recommended_fields(fields) do
    {order, required_map} =
      Enum.reduce(fields, {[], %{}}, fn field, {order, required_map} ->
        case Source.query_field_name(field) do
          "" ->
            {order, required_map}

          name ->
            required? = Source.required_query_field?(field)
            merged_required? = Map.get(required_map, name, false) or required?
            order = maybe_prepend_name(order, required_map, name)

            {order, Map.put(required_map, name, merged_required?)}
        end
      end)

    order
    |> Enum.reverse()
    |> Enum.map(&%{name: &1, required?: required_map[&1]})
  end

  defp maybe_prepend_name(order, required_map, name) do
    if Map.has_key?(required_map, name) do
      order
    else
      [name | order]
    end
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
    <div class="search-control tw-mt-1" id="source-logs-search-control" phx-hook="SourceLogsSearch">
      <.form :let={f} for={@search_form} action="#" phx-submit="start_search" phx-change="form_update" class="form-group">
        <.recommended_field_inputs fields={Source.recommended_query_fields(@source)} id_prefix="search-field" />

        <div class="form-group form-text">
          {text_input(f, :querystring,
            phx_focus: :form_focus,
            phx_blur: :form_blur,
            value: @querystring,
            class: "form-control form-control-margin tw-mt-1",
            list: "matches"
          )}
          {text_input(f, :search_timezone,
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

        <div class="d-flex flex-wrap align-items-center form-text">
          <div class="pr-2 pt-1 pb-1">
            <%= submit disabled: @loading, id: "search", class: "btn btn-primary" do %>
              <i class="fas fa-search"></i><span class="fas-in-button hide-on-mobile">Search</span>
            <% end %>
          </div>

          <.navigation_buttons tailing?={@tailing?} uri_params={@uri_params} />

          <.action_buttons source={@source} user={@user} has_results?={@has_results?} />
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

  attr :tailing?, :boolean, required: true
  attr :play_event, :string, values: ["soft_play", "hard_play"]

  def live_pause_button(assigns) do
    ~H"""
    <span :if={@tailing?} class="btn btn-primary live-pause mr-0 text-nowrap" phx-click="soft_pause">
      <i class="spinner-border spinner-border-sm text-info" role="status"></i>
      <span class="fas-in-button hide-on-mobile" id="search-tailing-button">Pause</span>
    </span>
    <span :if={not @tailing?} class="btn btn-primary live-pause mr-0" phx-click={@play_event}>
      <i class="fas fa-play"></i><span class="fas-in-button hide-on-mobile">Live</span>
    </span>
    """
  end

  attr :tailing?, :boolean, required: true
  attr :uri_params, :map, required: true

  def navigation_buttons(assigns) do
    assigns =
      assigns
      |> assign(
        :play_event,
        if(assigns.uri_params["tailing"] == "true",
          do: "soft_play",
          else: "hard_play"
        )
      )

    ~H"""
    <div class="btn-group pr-2">
      <a href="#" phx-click="backwards" class="btn btn-primary mr-0">
        <span class="fas fa-step-backward"></span>
      </a>
      <.live_pause_button tailing?={@tailing?} play_event={@play_event} />
      <a href="#" phx-click="forwards" class="btn btn-primary">
        <span class="fas fa-step-forward"></span>
      </a>
    </div>
    """
  end

  attr :source, Logflare.Sources.Source, required: true
  attr :user, Logflare.User, required: true
  attr :has_results?, :boolean

  def action_buttons(assigns) do
    ~H"""
    <div class="pr-2 pt-1 pb-1">
      <a href="#" phx-click="save_search" class="btn btn-primary">
        <i class="fas fa-bookmark"></i>
        <span class="fas-in-button hide-on-mobile">Save</span>
      </a>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <span class="btn btn-primary" id="daterangepicker">
        <i class="fas fa-clock"></i>
        <span class="hide-on-mobile fas-in-button">DateTime</span>
      </span>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <.link navigate={~p"/sources/#{@source}?querystring=c:count(*) c:group_by(t::minute)&tailing?=true"} class="btn btn-primary">
        <i class="fas fa-redo"></i>
        <span class="hide-on-mobile fas-in-button">Reset</span>
      </.link>
    </div>

    <div class="pr-2 pt-1 pb-1">
      <.create_menu user={@user} disabled={@has_results? == false} />
    </div>
    """
  end

  attr :user, Logflare.User, required: true
  attr :disabled, :boolean, default: false

  def create_menu(assigns) do
    ~H"""
    <.button_dropdown id="create-menu" disabled={@disabled}>
      <i class="fas fa-plus"></i>
      Create new...
      <:menu_item :if={Utils.flag("endpointsOpenBeta", @user)} heading="Endpoint">
        <.menu_link resource="endpoint" kind="events" />
      </:menu_item>
      <:menu_item :if={Utils.flag("endpointsOpenBeta", @user)}>
        <.menu_link resource="endpoint" kind="aggregates" />
      </:menu_item>
      <:menu_item :if={Utils.flag("alerts", @user)} heading="Alert">
        <.menu_link resource="alert" kind="events" />
      </:menu_item>
      <:menu_item :if={Utils.flag("alerts", @user)}>
        <.menu_link resource="alert" kind="aggregates" />
      </:menu_item>
      <:menu_item heading="Query">
        <.menu_link resource="query" kind="events" />
      </:menu_item>
      <:menu_item>
        <.menu_link resource="query" kind="aggregates" />
      </:menu_item>
    </.button_dropdown>
    """
  end

  attr :resource, :string, values: ["endpoint", "alert", "query"]
  attr :kind, :string, values: ["events", "aggregates"]

  defp menu_link(assigns) do
    assigns =
      assigns
      |> assign_new(:label, fn
        %{kind: "events"} -> "From search"
        %{kind: "aggregates"} -> "From chart"
      end)

    ~H"""
    <a phx-click="create_new" phx-value-resource={@resource} phx-value-kind={@kind} class="tw-block tw-text-gray-500 tw-no-underline" href="#">{@label}</a>
    """
  end
end
