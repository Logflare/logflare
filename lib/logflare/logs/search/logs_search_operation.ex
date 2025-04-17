defmodule Logflare.Logs.SearchOperation do
  @moduledoc """
  Logs search options and result
  """
  alias Logflare.Lql
  use TypedStruct

  typedstruct do
    field :source, Source.t()
    field :partition_by, :pseudo | :timestamp, enforce: true
    field :querystring, String.t(), enforce: true
    field :query, Ecto.Query.t()
    field :query_result, term()
    field :sql_params, {term(), term()}
    field :sql_string, String.t()
    field :tailing?, boolean, enforce: true
    field :tailing_initial?, boolean
    field :rows, [map()], default: []
    field :lql_meta_and_msg_filters, [FilterRule.t()], default: []
    field :lql_ts_filters, [FilterRule.t()], default: []
    field :lql_rules, [FilterRule.t() | ChartRule.t()]
    field :chart_rules, [ChartRule.t()], default: []
    field :error, term()
    field :stats, :map
    field :search_timezone, String.t()
    field :chart_data_shape_id, atom(), default: nil, enforce: true
    field :type, :events | :aggregates
    field :status, {atom(), String.t() | [String.t()]}
  end

  def new(params) do
    so = struct(__MODULE__, params)

    filter_rules = Lql.Utils.get_filter_rules(so.lql_rules)
    chart_rules = Lql.Utils.get_chart_rules(so.lql_rules)
    ts_filters = Lql.Utils.get_ts_filters(filter_rules)
    lql_meta_and_msg_filters = Lql.Utils.get_meta_and_msg_filters(filter_rules)

    %{
      so
      | lql_meta_and_msg_filters: lql_meta_and_msg_filters,
        chart_rules: chart_rules,
        lql_ts_filters: ts_filters
    }
  end
end
