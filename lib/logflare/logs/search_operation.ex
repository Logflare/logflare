defmodule Logflare.Logs.SearchOperation do
  @moduledoc """
  Logs search options and result
  """
  use TypedStruct

  alias Logflare.Backends
  alias Logflare.Lql.Rules, as: LqlRules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Sources.Source

  typedstruct do
    field :source, Source.t()
    field :source_token, atom()
    field :source_id, number()
    field :backend_type, :bigquery | :postgres, default: :bigquery
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
    field :lql_rules, [ChartRule.t() | FilterRule.t() | SelectRule.t()]
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

    chart_rules = LqlRules.get_chart_rules(so.lql_rules)
    ts_filters = LqlRules.get_timestamp_filters(so.lql_rules)
    lql_meta_and_msg_filters = LqlRules.get_metadata_and_message_filters(so.lql_rules)

    %{
      so
      | lql_meta_and_msg_filters: lql_meta_and_msg_filters,
        chart_rules: chart_rules,
        lql_ts_filters: ts_filters,
        source_token: so.source.token,
        source_id: so.source.id,
        backend_type: resolve_backend_type(so)
    }
  end

  @spec resolve_backend_type(t()) :: :bigquery | :postgres
  defp resolve_backend_type(%__MODULE__{source: %{user: user}})
       when is_struct(user, Logflare.User) do
    case Backends.get_default_backend(user) do
      %{type: :postgres} -> :postgres
      _ -> :bigquery
    end
  end

  defp resolve_backend_type(%__MODULE__{}), do: :bigquery
end
