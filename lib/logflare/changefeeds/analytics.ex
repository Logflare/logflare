defmodule Logflare.Changefeeds.Analytics do
  use Logflare.Commons
  alias Logflare.Mnesia

  @type indexes_aggregate_metrics :: %{
          memory: integer(),
          memory_avg: integer(),
          count: integer()
        }

  defmodule Changefeed.Metrics do
    use TypedStruct

    typedstruct do
      field :memory_total, integer(), default: 0
      field :memory_tables, integer(), default: 0
      field :memory_derived_tables, integer(), default: 0
      field :memory_indexes, integer(), default: 0
      field :table_count, integer(), default: 0
    end
  end

  defmodule ChangefeedData do
    use TypedStruct

    typedstruct do
      field :table, String.t()
      field :memory, integer()
      field :type, atom()
      field :storage_type, atom()
      field :attributes, [atom()]
      field :size, integer()
      field :indexes, [Index.t()]
      field :indexes_aggregate, term()
    end
  end

  def run() do
    changefeed_metrics = changefeed_metrics()

    %{
      metrics: node_changefeed_metrics(changefeed_metrics),
      changefeeds: changefeed_metrics
    }
  end

  @spec node_changefeed_metrics([Changefeed.Data]) :: Changefeed.Metrics
  def node_changefeed_metrics(changefeed_data) when is_list(changefeed_data) do
    metrics =
      changefeed_data
      |> Enum.reduce(%Changefeed.Metrics{}, fn %ChangefeedData{
                                                 memory: mem,
                                                 size: _size,
                                                 indexes_aggregate: indexes_aggregate
                                               },
                                               %Changefeed.Metrics{} = acc ->
        %{
          acc
          | memory_tables: acc.memory_total + mem,
            memory_indexes: acc.memory_indexes + indexes_aggregate.memory
        }
      end)

    %{memory_tables: mt, memory_indexes: mi} = metrics
    %{metrics | memory_total: mt + mi, table_count: Enum.count(changefeed_data)}
  end

  @spec changefeed_metrics() :: Changefeed.Metrics.t()
  def changefeed_metrics() do
    for %{table: table} <- Changefeeds.list_changefeed_subscriptions() do
      tab = String.to_existing_atom(table)

      mnesia_info =
        tab
        |> Mnesia.table_info(:all)
        |> Map.new()

      indexes =
        tab
        |> Mnesia.Indexes.list_indexes()
        |> Mnesia.Indexes.refresh_index_data()

      %ChangefeedData{
        table: table,
        memory: mnesia_info.memory,
        type: mnesia_info.type,
        storage_type: mnesia_info.storage_type,
        attributes: mnesia_info.attributes,
        size: mnesia_info.size,
        indexes: indexes,
        indexes_aggregate: sum_index_values(indexes)
      }
    end
  end

  @spec fetch_analytics_all_nodes() :: [map]
  def fetch_analytics_all_nodes() do
    nodes = [Node.self() | Node.list()]
    analytics = Enum.map(nodes, &fetch_analytics/1)

    nodes
    |> Enum.zip(analytics)
    |> Map.new()
  end

  @spec fetch_analytics(atom()) :: atom()
  def fetch_analytics(node) when node() != node do
    :erpc.call(node, __MODULE__, :fetch_analytics, [])
  end

  def fetch_analytics(node) when node() == node do
    changefeed_metrics = changefeed_metrics()

    %{
      metrics: node_changefeed_metrics(changefeed_metrics),
      changefeeds: changefeed_metrics
    }
  end

  @spec sum_index_values([Mnesia.Indexes.Index.t()]) :: indexes_aggregate_metrics
  def sum_index_values(indexes) when is_list(indexes) do
    count = Enum.count(indexes)
    memory = indexes |> Enum.map(& &1.memory) |> Enum.sum()
    memory_avg = div(memory, count)

    %{count: count, memory: memory, memory_avg: memory_avg}
  end
end
