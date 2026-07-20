defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component
  use LogflareWeb, :html
  use LogflareWeb, :routes

  import LogflareWeb.ErrorHelpers
  import Phoenix.HTML.Form

  alias Logflare.Backends.Backend
  alias Logflare.Logs.SearchOperations.Helpers
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Sql
  alias LogflareWeb.Utils
  alias Phoenix.LiveView.JS

  attr :backends, :list, required: true
  attr :form, :any, required: true
  attr :field, :atom, default: :backend_id
  attr :show_language, :boolean, default: true
  attr :label, :string, default: nil
  attr :default_backend, Backend, required: true

  slot :help

  def backend_select(assigns) do
    assigns =
      assign_new(assigns, :options, fn %{backends: backends, default_backend: default_backend} ->
        backend_options = Enum.map(backends, &{"#{&1.name} (#{&1.type})", &1.id})
        [{default_backend.name, nil}] ++ backend_options
      end)

    ~H"""
    <div class="form-group">
      <label :if={@label}>{@label}</label>
      <small :if={render_slot(@help)} class="form-text text-muted">{render_slot(@help)}</small>
      <select id={@form[@field].id} name={@form[@field].name} class="form-control">
        {options_for_select(@options, @form[@field].value)}
      </select>
      {error_tag(@form, @field)}
      <div :if={@show_language} class="tw-mt-2">
        <strong>Query Language: <span id="query-language">{format_query_language(@form[@field].value, @backends, @default_backend)}</span></strong>
      </div>
    </div>
    """
  end

  defp format_query_language(nil, _backends, default_backend),
    do: format_backend_language(default_backend)

  defp format_query_language(backend_id, backends, _default_backend) do
    backend = Enum.find(backends, &(to_string(&1.id) == to_string(backend_id)))
    format_backend_language(backend)
  end

  defp format_backend_language(%Backend{type: :clickhouse}), do: "ClickHouse SQL"
  defp format_backend_language(%Backend{type: :postgres}), do: "Postgres SQL"
  defp format_backend_language(_), do: "BigQuery SQL"

  attr :bytes, :integer, default: nil

  def query_cost(assigns) do
    {size, unit} = Utils.humanize_bytes(assigns.bytes)

    assigns =
      assigns
      |> assign(unit: unit)
      |> assign(size: Decimal.from_float(size) |> Decimal.normalize())

    ~H"""
    <div :if={is_number(@bytes)} class="tw-text-sm">
      {@size} {@unit} processed
    </div>
    """
  end

  attr :lql, :string, required: true
  attr :node, :map, required: true
  attr :source, :map, required: true
  attr :source_schema_flat_map, :map, required: true
  attr :lql_schema, :map, required: true
  attr :search_params, :map, default: %{}
  attr :team, :any, default: nil

  def quick_filter(assigns) do
    include_path = append_to_query(assigns)
    exclude_path = append_to_query(assigns, :exclude)

    assigns =
      assigns
      |> assign(:include_path, include_path)
      |> assign(:exclude_path, exclude_path)
      |> assign(:value_ok?, quick_filter_value_ok?(assigns.node))
      |> assign_new(:class, fn %{node: %{path: [path | _]}} ->
        if path in ~w(id event_message timestamp) do
          "tw-visible"
        end
      end)

    ~H"""
    <div class="tw-inline-block tw-space-x-1">
      <.team_link :if={@include_path && @value_ok?} team={@team} title="Append to query" class="tw-no-underline tw-invisible group-hover:tw-visible" patch={@include_path}>
        <i class={["fas fa-search tw-text-xs", @class]}></i>
        <span>Include</span>
      </.team_link>
      <.team_link :if={@exclude_path && @value_ok?} team={@team} title="Exclude from query" class="tw-no-underline tw-invisible group-hover:tw-visible" patch={@exclude_path}>
        <i class="fa fa-ban tw-text-xs"></i>
        <span>Exclude</span>
      </.team_link>
    </div>
    """
  end

  attr :sql_string, :string
  attr :params, :list, default: [], doc: "List of %GoogleApi.BigQuery.V2.Model.QueryParameter{}"

  def formatted_sql(assigns) do
    assigns =
      assigns
      |> assign_new(:formatted_sql, fn ->
        {:ok, formatted} =
          Utils.sql_params_to_sql(assigns.sql_string, assigns.params)
          |> prepare_table_name()
          |> Sql.format()

        formatted
      end)

    ~H"""
    <div class="tw-group tw-relative">
      <.link
        class="tw-text-[0.65rem] tw-py-1 tw-px-2 tw-rounded
        tw-text-sm
        group-hover:tw-visible tw-invisible tw-absolute tw-capitalize tw-top-0 tw-right-0 tw-bg-[#6c757d] tw-hover:tw-bg-[#5a6268] tw-text-white tw-no-underline"
        phx-click={
          JS.dispatch("logflare:copy-to-clipboard",
            detail: %{
              text: @formatted_sql
            }
          )
        }
        data-toggle="tooltip"
        data-placement="top"
        title="Copy to clipboard"
      >
        <i class="fa fa-clone" aria-hidden="true"></i> copy
      </.link>
      <pre class="tw-flex-grow"><code class="sql"><%= @formatted_sql %></code></pre>
    </div>
    """
  end

  defp prepare_table_name(sql) do
    Regex.replace(
      ~r/`([^`]+)`\.([^\s]+)/,
      sql,
      fn _, project, rest ->
        "`#{project}.#{rest}`"
      end
    )
  end

  @spec append_to_query(map(), :include | :exclude) :: String.t() | nil
  def append_to_query(
        %{
          lql: lql,
          node: %{path: path, value: value},
          source: source,
          source_schema_flat_map: flat_map,
          lql_schema: lql_schema,
          search_params: search_params
        } = assigns,
        action \\ :include
      ) do
    team = Map.get(assigns, :team)

    case lookup_schema_path(path, flat_map) do
      {normalized_key, list_includes?} ->
        resolved_path = resolve_lql_path(normalized_key, flat_map)

        updated_lql =
          lql
          |> Lql.decode!(lql_schema)
          |> upsert_filter_rule(resolved_path, value, list_includes?, action)
          |> Lql.encode!()

        params =
          search_params
          |> Map.take(["tz"])
          |> Map.merge(%{querystring: updated_lql, tailing?: false})

        ~p"/sources/#{source}/search?#{params}"
        |> Utils.with_team_param(team)

      nil ->
        nil
    end
  end

  defp upsert_filter_rule(rules, "timestamp", value, _list_includes?, action) do
    chart_period = Lql.Rules.get_chart_period(rules, :minute)

    ts_rule =
      normalize_filter_rule_value("timestamp", value)
      |> build_timestamp_filter_rule(chart_period)
      |> maybe_negate_filter_rule(action)

    Lql.Rules.upsert_filter_rule_by_path(rules, ts_rule)
  end

  defp upsert_filter_rule(rules, path, value, list_includes?, action) do
    filter_rule =
      FilterRule.build(
        path: path,
        operator: if(list_includes?, do: :list_includes, else: :=),
        value: value,
        modifiers: if(is_binary(value), do: %{quoted_string: true}, else: %{})
      )
      |> maybe_negate_filter_rule(action)

    Lql.Rules.upsert_filter_rule_by_path(rules, filter_rule)
  end

  defp maybe_negate_filter_rule(%FilterRule{} = rule, :exclude) do
    %{rule | modifiers: Map.put(rule.modifiers, :negate, true)}
  end

  defp maybe_negate_filter_rule(%FilterRule{} = rule, :include), do: rule

  @doc """
  Looks up the path in the schema flat map and returns whether it's a list type.

  ## Examples

      iex> lookup_schema_path(["metadata", "tags"], %{"metadata.tags" => {:list, :string}})
      {"metadata.tags", true}
      iex> lookup_schema_path(["metadata", "0", "status"], %{"metadata.0.status" => :string})
      {"metadata.0.status", false}
      iex> lookup_schema_path(["metadata", "missing"], %{"metadata.status" => :string})
      nil
  """
  @spec lookup_schema_path([String.t()], map()) :: {String.t(), boolean()} | nil
  def lookup_schema_path(_path, nil), do: nil

  def lookup_schema_path(path, flat_map) do
    keypath = Enum.join(path, ".")

    case Map.get(flat_map, keypath) do
      {:list, _} -> {keypath, true}
      nil -> nil
      _ -> {keypath, false}
    end
  end

  defp resolve_lql_path(key, schema_flat_map) do
    if Map.has_key?(schema_flat_map, key) do
      key
    else
      "metadata.#{key}"
    end
  end

  defp quick_filter_value_ok?(%{value: value}) when is_binary(value) do
    String.length(value) <= 500
  end

  defp quick_filter_value_ok?(_node), do: true

  defp build_timestamp_filter_rule(value, chart_period) do
    {min_ts, max_ts} = timestamp_range(value, chart_period)

    FilterRule.build(
      path: "timestamp",
      operator: :range,
      values: [min_ts, max_ts]
    )
  end

  defp normalize_filter_rule_value("timestamp", value) when is_integer(value) do
    value
    |> Logflare.Utils.to_microseconds()
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.to_naive()
  end

  defp normalize_filter_rule_value(_path, value), do: value

  defp timestamp_range(%NaiveDateTime{} = timestamp, chart_period) do
    shift_key = Helpers.to_timex_shift_key(chart_period)

    {
      Timex.shift(timestamp, [{shift_key, -1}]),
      Timex.shift(timestamp, [{shift_key, 1}])
    }
  end
end
