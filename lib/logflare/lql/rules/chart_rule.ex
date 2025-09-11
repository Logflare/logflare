defmodule Logflare.Lql.Rules.ChartRule do
  @moduledoc """
  Represents a chart aggregation rule in LQL for generating time-series charts and analytics.

  `ChartRule` defines how to aggregate log data over time periods to create visual charts
  and statistical summaries. It supports various aggregation functions and time grouping
  periods for analyzing trends in log data.

  ## Basic Chart Examples:
  - `c:count(*)` - count all events over time (default: by minute)
  - `c:avg(m.latency)` - average latency values over time
  - `c:sum(m.request_size)` - sum of request sizes over time
  - `c:max(m.response_time)` - maximum response time over time

  ## Percentile Aggregations:
  - `c:p50(m.latency)` - 50th percentile (median) of latency
  - `c:p95(m.latency)` - 95th percentile of latency
  - `c:p99(m.latency)` - 99th percentile of latency

  ## Time Grouping Examples:
  - `c:group_by(t::second)` or `c:group_by(t::s)` - group by seconds
  - `c:group_by(t::minute)` or `c:group_by(t::m)` - group by minutes (default)
  - `c:group_by(t::hour)` or `c:group_by(t::h)` - group by hours
  - `c:group_by(t::day)` or `c:group_by(t::d)` - group by days

  ## Combined Chart Syntax:
  - `c:avg(m.latency) c:group_by(t::hour)` - average latency per hour
  - `c:sum(m.bytes) c:group_by(t::minute)` - sum bytes per minute
  - `c:p95(m.duration) c:group_by(t::day)` - 95th percentile duration per day

  ## Nested Field Aggregations:
  - `c:avg(m.user.session.duration)` - aggregate deeply nested numeric field
  - `c:count(m.request.headers.user_agent)` - count events with user agent data
  - `c:max(m.database.query.execution_time)` - maximum query execution time

  ## Supported Aggregation Functions:
  - `:count` - count of events (works with `*` or any field)
  - `:avg` - arithmetic mean of numeric values
  - `:sum` - sum of numeric values
  - `:max` - maximum value
  - `:p50` - 50th percentile (median)
  - `:p95` - 95th percentile
  - `:p99` - 99th percentile

  ## Supported Time Periods:
  - `:second` - group events by second intervals
  - `:minute` - group events by minute intervals (default)
  - `:hour` - group events by hour intervals
  - `:day` - group events by day intervals

  ## Field Structure:
  - `path` - field path to aggregate (e.g., "metadata.latency", defaults to "timestamp")
  - `aggregate` - aggregation function atom (e.g., :count, :avg, :sum, :max, :p95)
  - `period` - time grouping period atom (e.g., :minute, :hour, :day)
  - `value_type` - data type of the field being aggregated (used for validation)

  ## Usage Notes:
  - Chart rules are typically used with filtering rules for focused analytics
  - Multiple chart rules can be combined with different aggregations
  - Time periods determine chart granularity and data point density
  - Percentile functions are useful for performance monitoring and SLA tracking
  """
  use TypedEctoSchema

  import Ecto.Changeset

  alias Ecto.Changeset

  @derive {Jason.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true, default: "timestamp"
    field :value_type, Ecto.Atom, virtual: true
    field :period, Ecto.Atom, virtual: true, default: :minute
    field :aggregate, Ecto.Atom, virtual: true, default: :count
  end

  @spec changeset(any(), __MODULE__.t()) :: Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, virtual_fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Changeset.t()
  def changeset(%__MODULE__{} = rule, params) do
    cast(rule, params, virtual_fields())
  end

  @spec build(list()) :: __MODULE__.t()
  def build(params) when is_list(params) do
    changeset = changeset(%__MODULE__{}, Map.new(params))

    case changeset do
      %{valid?: true} ->
        Changeset.apply_changes(changeset)

      %{valid?: false} ->
        %__MODULE__{}
    end
  end

  @spec build_from_path(String.t()) :: __MODULE__.t()
  def build_from_path(path) do
    build(path: path)
  end

  @spec virtual_fields() :: list(atom())
  def virtual_fields do
    __MODULE__.__schema__(:virtual_fields)
  end

  # =============================================================================
  # Rule-Specific Operations
  # =============================================================================

  @doc """
  Extracts the period field from a `ChartRule`.
  """
  @spec get_period(__MODULE__.t()) :: atom()
  def get_period(%__MODULE__{period: period}), do: period

  @doc """
  Extracts the aggregate field from a `ChartRule`.
  """
  @spec get_aggregate(__MODULE__.t()) :: atom()
  def get_aggregate(%__MODULE__{aggregate: aggregate}), do: aggregate

  @doc """
  Updates a `ChartRule` with the provided parameters map using changeset validation.
  """
  @spec update(__MODULE__.t(), map()) :: __MODULE__.t()
  def update(%__MODULE__{} = chart_rule, params) when is_map(params) do
    changeset = changeset(chart_rule, params)

    case changeset do
      %{valid?: true} ->
        Changeset.apply_changes(changeset)

      %{valid?: false} ->
        chart_rule
    end
  end
end
