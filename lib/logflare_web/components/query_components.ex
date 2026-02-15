defmodule LogflareWeb.QueryComponents do
  use Phoenix.Component
  use LogflareWeb, :routes

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Logs.SearchOperations.Helpers
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SourceSchemas.Cache
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
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
  attr :is_tailing, :boolean, default: false

  def quick_filter(assigns) do
    assigns =
      assigns
      |> assign(
        :path,
        append_to_query(assigns.lql, assigns.node, assigns.source, assigns.is_tailing)
      )
      |> assign(:value_ok?, quick_filter_value_ok?(assigns.node))

    ~H"""
    <.link :if={@path && @value_ok?} title="Append to query" patch={@path}>
      <i class="fas fa-search tw-mr-1 tw-w-2"></i>
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

  @spec append_to_query(String.t(), map(), Logflare.Sources.Source.t(), boolean()) ::
          String.t() | nil
  def append_to_query(lql, %{key: key, path: path, value: value}, source, is_tailing) do
    flat_map = source_schema_flat_map(source)

    case normalize_array_key(key, path, flat_map) do
      {normalized_key, list_includes?} ->
        resolved_path = resolve_lql_path(normalized_key, flat_map)
        updated_lql = append_filter(lql, resolved_path, value, source, list_includes?)

        is_tailing = if key == "timestamp", do: false, else: is_tailing

        ~p"/sources/#{source}/search?#{%{querystring: updated_lql, tailing?: is_tailing}}"

      nil ->
        nil
    end
  end

  defp append_filter(lql, path, value, source, list_includes?) do
    lql_rules = Lql.decode!(lql, lql_schema(source))
    chart_period = Lql.Rules.get_chart_period(lql_rules, :minute)
    value = normalize_timestamp_value(path, value)

    lql_rules
    |> updated_lql(path, value, list_includes?, chart_period)
    |> Lql.encode!()
  end

  defp updated_lql(rules, path, value, list_includes?, chart_period) do
    rules
    |> maybe_drop_timestamp_filters(path)
    |> add_filter_rule(path, value, list_includes?, chart_period)
  end

  defp maybe_drop_timestamp_filters(rules, "timestamp") do
    Enum.reject(rules, &match?(%FilterRule{path: "timestamp"}, &1))
  end

  defp maybe_drop_timestamp_filters(rules, _path), do: rules

  defp add_filter_rule(rules, "timestamp", value, _list_includes?, chart_period) do
    {min_ts, max_ts} = timestamp_range(value, chart_period)

    filter_rule =
      FilterRule.build(
        path: "timestamp",
        operator: :range,
        values: [min_ts, max_ts]
      )

    rules ++ [filter_rule]
  end

  defp add_filter_rule(rules, path, value, list_includes?, _chart_period) do
    filter_rule =
      FilterRule.build(
        path: path,
        operator: if(list_includes?, do: :list_includes, else: :=),
        value: value,
        modifiers: if(is_binary(value), do: %{quoted_string: true}, else: %{})
      )

    rules ++ [filter_rule]
  end

  defp lql_schema(source) do
    case Cache.get_source_schema_by(source_id: source.id) do
      %_{bigquery_schema: schema} when not is_nil(schema) -> schema
      _ -> SchemaBuilder.initial_table_schema()
    end
  end

  @doc """
  Checks the path + key exists in the schema.
  If it doesn't the keypath may be an array index (eg. `tags.0`), so
  recursively drops the last path segment until it finds an array type.
  """
  @spec normalize_array_key(String.t(), [String.t()], map()) :: {String.t(), boolean()} | nil
  def normalize_array_key(key, path, flat_map) do
    keypath = (path ++ [key]) |> Enum.join(".")

    case Map.get(flat_map, keypath) do
      {:list, _} ->
        {keypath, true}

      nil ->
        walk_up_for_array(path, flat_map)

      _ ->
        {keypath, false}
    end
  end

  defp walk_up_for_array([], _flat_map), do: nil

  defp walk_up_for_array(path, flat_map) do
    keypath = Enum.join(path, ".")

    case Map.get(flat_map, keypath) do
      {:list, _} -> {keypath, true}
      nil -> walk_up_for_array(Enum.drop(path, -1), flat_map)
      _ -> nil
    end
  end

  defp resolve_lql_path(key, schema_flat_map) do
    if Map.has_key?(schema_flat_map, key) do
      key
    else
      "metadata.#{key}"
    end
  end

  defp source_schema_flat_map(source) do
    case Cache.get_source_schema_by(source_id: source.id) do
      %_{schema_flat_map: flatmap} when is_map(flatmap) -> flatmap
      _ -> SchemaBuilder.initial_table_schema() |> SchemaUtils.bq_schema_to_flat_typemap()
    end
  end

  defp quick_filter_value_ok?(%{value: value}) when is_binary(value) do
    String.length(value) <= 500
  end

  defp quick_filter_value_ok?(_node), do: true

  defp normalize_timestamp_value("timestamp", value) when is_integer(value) do
    value
    |> Logflare.Utils.to_microseconds()
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.to_naive()
  end

  defp normalize_timestamp_value(_path, value), do: value

  defp timestamp_range(%NaiveDateTime{} = timestamp, chart_period) do
    shift_key = Helpers.to_timex_shift_key(chart_period)

    {
      Timex.shift(timestamp, [{shift_key, -1}]),
      Timex.shift(timestamp, [{shift_key, 1}])
    }
  end
end
