defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component
  use LogflareWeb, :routes

  alias Logflare.Logs.SearchOperations.Helpers
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias LogflareWeb.Utils
  alias Phoenix.LiveView.JS

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

  def quick_filter(%{node: %{path: [path]}} = assigns)
      when path in ["event_message", "timestamp"] and not is_map_key(assigns, :class) do
    assigns
    |> assign(:class, nil)
    |> quick_filter()
  end

  def quick_filter(assigns) do
    assigns =
      assigns
      |> assign(
        :path,
        append_to_query(
          assigns.lql,
          assigns.node,
          assigns.source,
          assigns.source_schema_flat_map,
          assigns.lql_schema
        )
      )
      |> assign(:value_ok?, quick_filter_value_ok?(assigns.node))
      |> assign_new(:class, fn -> "tw-hidden group-hover:tw-inline" end)

    ~H"""
    <.link :if={@path && @value_ok?} title="Append to query" class={@class} patch={@path}>
      <i class="fas fa-search tw-text-xs tw-mr-1 tw-w-2"></i>
    </.link>
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
          |> SqlFmt.format_query()

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

  @spec append_to_query(String.t(), map(), Logflare.Sources.Source.t(), map(), map()) ::
          String.t() | nil
  def append_to_query(lql, %{path: path, value: value}, source, flat_map, lql_schema) do
    case lookup_schema_path(path, flat_map) do
      {normalized_key, list_includes?} ->
        resolved_path = resolve_lql_path(normalized_key, flat_map)

        updated_lql =
          lql
          |> Lql.decode!(lql_schema)
          |> upsert_filter_rule(resolved_path, value, list_includes?)
          |> Lql.encode!()

        ~p"/sources/#{source}/search?#{%{querystring: updated_lql, tailing?: false}}"

      nil ->
        nil
    end
  end

  defp upsert_filter_rule(rules, "timestamp", value, _list_includes?) do
    chart_period = Lql.Rules.get_chart_period(rules, :minute)

    ts_rule =
      normalize_filter_rule_value("timestamp", value)
      |> build_timestamp_filter_rule(chart_period)

    Lql.Rules.upsert_filter_rule_by_path(rules, ts_rule)
  end

  defp upsert_filter_rule(rules, path, value, list_includes?) do
    filter_rule =
      FilterRule.build(
        path: path,
        operator: if(list_includes?, do: :list_includes, else: :=),
        value: value,
        modifiers: if(is_binary(value), do: %{quoted_string: true}, else: %{})
      )

    Lql.Rules.upsert_filter_rule_by_path(rules, filter_rule)
  end

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
