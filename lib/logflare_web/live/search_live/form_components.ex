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

  attr :lql_rules, :list, required: true
  attr :chart_aggregate_enabled?, :boolean, required: true

  def chart_controls(assigns) do
    ~H"""
    <div class="d-flex flex-wrap align-items-center form-text pt-2">
      <div class="pr-3 pt-1 pb-1  hide-on-mobile">
        Chart period:
      </div>
      <div class="pr-3 pt-1 pb-1">
        <select id="search_chart_period" name="chart_period" class="form-control form-control-sm">
          {Phoenix.HTML.Form.options_for_select(
            ["day", "hour", "minute", "second"],
            Rules.get_chart_period(@lql_rules, "minute")
          )}
        </select>
      </div>
      <div class="pr-3 pt-1 pb-1 hide-on-mobile">
        Aggregate:
      </div>
      <div class="pr-3 pt-1 pb-1">
        <%= if @chart_aggregate_enabled? do %>
          <select id="search_chart_aggregate" name="chart_aggregate" class="form-control form-control-sm">
            {Phoenix.HTML.Form.options_for_select(
              ["sum", "avg", "max", "p50", "p95", "p99", "count"],
              Rules.get_chart_aggregate(@lql_rules, "count")
            )}
          </select>
        <% else %>
          <span class="d-inline-block" tabindex="0" data-toggle="tooltip" title="Chart aggregate setting requires usage of chart: operator" trigger="hover click" delay="0">
            <select id="search_chart_aggregate" name="chart_aggregate" class="form-control form-control-sm" style="pointer-events: none;">
              {Phoenix.HTML.Form.options_for_select(["count"], "count")}
            </select>
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
    <div :if={Enum.any?(@fields)} class="form-text tw-order-1 tw-basis-full sm:tw-basis-auto sm:tw-shrink-0" id="recommended_fields" phx-update="ignore">
      <div class="sm:tw-flex tw-items-end tw-gap-2">
        <div :for={field <- @fields} class="recommended-field-container tw-basis-full tw-min-w-0 sm:tw-min-w-20 sm:tw-max-w-48 tw-mb-2 sm:tw-mb-0">
          <div class="tw-flex tw-justify-between tw-items-baseline">
            <label for={"#{@id_prefix}-#{field.name}"} data-toggle="tooltip" title={field.name} class="logflare-tooltip tw-truncate tw-mb-0 tw-text-xs tw-text-gray-300 tw-block">{field.name}</label>
            <span :if={field.required?} class="required-field-indicator tw-text-gray-500 tw-block tw-text-right tw-text-xs">
              required
            </span>
          </div>
          <input
            id={"#{@id_prefix}-#{field.name}"}
            name={input_name(:fields, field.name)}
            class="form-control tw-h-8 tw-min-h-8 tw-max-h-8 tw-border-[#282c34] tw-bg-[#282c34] tw-py-[3px] tw-text-sm tw-font-mono tw-text-[#c4cad6] placeholder:tw-text-[#8c92a3] focus:tw-border-[#3e4451] focus:tw-bg-[#282c34] focus:tw-text-[#c4cad6]"
            type="text"
          />
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

  attr :querystring, :string, required: true
  attr :lql_schema_fields_json, :string, required: true
  attr :saved_searches, :list, required: true
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
    assigns =
      assigns
      |> assign(:saved_searches_json, JSON.encode!(assigns.saved_searches))

    ~H"""
    <div class="search-control tw-mt-1" id="source-logs-search-control" phx-hook="SourceLogsSearch">
      <div class="form-group">
        <div class="form-group form-text">
          <div class="tw-flex tw-flex-wrap tw-items-end tw-gap-2">
            <.recommended_field_inputs fields={Source.recommended_query_fields(@source)} id_prefix="search-field" />

            <div class="tw-order-2 tw-basis-full tw-min-w-0 sm:tw-min-w-[20rem] sm:tw-basis-0 sm:tw-flex-1">
              <div id="lql-editor-hook" phx-hook="LqlEditorWrapper" phx-update="ignore" data-querystring={@querystring} data-schema-fields-json={@lql_schema_fields_json} data-suggested-searches-json={@saved_searches_json} class="lql-editor-wrapper tw-mt-0">
                <LiveMonacoEditor.code_editor value={@querystring} path="lql_query" class="tw-w-full tw-h-8" opts={lql_editor_opts()} />
              </div>
            </div>
          </div>
        </div>

        <div class="d-flex flex-wrap align-items-center form-text">
          <div class="pr-2 pt-1 pb-1">
            <button type="button" disabled={@loading} id="search" class="btn btn-primary" phx-click={Phoenix.LiveView.JS.dispatch("lql:submit", to: "#lql-editor-hook")}>
              <i class="fas fa-search"></i><span class="fas-in-button hide-on-mobile">Search</span>
            </button>
          </div>

          <.navigation_buttons tailing?={@tailing?} uri_params={@uri_params} />

          <.action_buttons source={@source} user={@user} has_results?={@has_results?} />
        </div>

        <.chart_controls lql_rules={@lql_rules} chart_aggregate_enabled?={search_agg_controls_enabled?(@lql_rules)} />

        <div id="observer-target"></div>
      </div>

      <.query_timing last_query_completed_at={@last_query_completed_at} />
    </div>
    """
  end

  defp lql_editor_opts do
    Map.merge(
      LiveMonacoEditor.default_opts(),
      %{
        "language" => "lql",
        "lineNumbers" => "off",
        "glyphMargin" => false,
        "folding" => false,
        "lineDecorationsWidth" => 0,
        "lineNumbersMinChars" => 0,
        "wordWrap" => "off",
        "scrollbar" => %{
          "horizontal" => "hidden",
          "vertical" => "hidden",
          "handleMouseWheel" => false
        },
        "overviewRulerLanes" => 0,
        "overviewRulerBorder" => false,
        "hideCursorInOverviewRuler" => true,
        "contextmenu" => false,
        "fixedOverflowWidgets" => true,
        "suggest" => %{"enabled" => true, "showWords" => false},
        "parameterHints" => %{"enabled" => false},
        "quickSuggestions" => true,
        "matchBrackets" => "never",
        "tabIndex" => 0,
        "fontFamily" =>
          "SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
        "padding" => %{"top" => 5, "bottom" => 5},
        "automaticLayout" => true
      }
    )
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
