defmodule Logflare.Logs.SearchOperation do
  @moduledoc """
  Logs search options and result
  """
  use TypedStruct

  typedstruct do
    field :source, Source.t()
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
    field :chart_rules, [ChartRule.t()], default: []
    field :error, term()
    field :stats, :map
    field :use_local_time, boolean
    field :user_local_timezone, String.t()
    field :chart_period, atom(), default: :minute, enforce: true
    field :chart_aggregate, atom(), default: :count, enforce: true
    field :chart_data_shape_id, atom(), default: nil, enforce: true
    field :type, :events | :aggregates
    field :status, {atom(), String.t() | [String.t()]}
  end
end
