defmodule Logflare.LiveDashboard.LocalRepoPage do
  @moduledoc false
  alias Logflare.Changefeeds
  use Phoenix.LiveDashboard.PageBuilder

  @impl true
  def menu_link(_, _) do
    {:ok, "LocalRepo"}
  end

  @impl true
  def render_page(_assigns) do
    nav_bar(
      items: [
        nodes: [
          name: "Nodes",
          render:
            table(
              columns: columns(:nodes),
              id: :local_repo_nodes,
              row_attrs: fn table -> row_attrs(table, :nodes) end,
              row_fetcher: fn params, node -> fetch_analytics(params, node, :nodes) end,
              rows_name: "nodes",
              title: "LocalRepo"
            )
        ],
        tables: [
          name: "Tables",
          render:
            table(
              columns: columns(:changefeed_tables),
              id: :local_repo_tables,
              row_attrs: fn table -> row_attrs(table, :changefeed_tables) end,
              row_fetcher: fn params, node ->
                fetch_analytics(params, node, :changefeed_tables)
              end,
              rows_name: "nodes",
              title: "LocalRepo"
            )
        ]
      ]
    )
  end

  defp fetch_analytics(_params, _node, :nodes) do
    node_analytics =
      Changefeeds.Analytics.fetch_analytics_all_nodes()
      |> Enum.map(fn {node, %{metrics: metrics}} -> Map.put(metrics, :node, node) end)
      |> Enum.map(&Map.from_struct/1)

    {node_analytics, Enum.count(node_analytics)}
  end

  defp fetch_analytics(_params, _node, :changefeed_tables) do
    tables =
      Changefeeds.Analytics.fetch_analytics_all_nodes()
      |> Enum.flat_map(fn {node, %{changefeeds: changefeeds}} ->
        Enum.map(changefeeds, &Map.put(&1, :node, node))
      end)
      |> Enum.map(&Map.from_struct/1)
      |> Enum.map(&Map.put(&1, :indexes_memory, &1.indexes_aggregate.memory))

    {tables, Enum.count(tables)}
  end

  defp columns(:changefeed_tables) do
    [
      %{
        field: :table,
        header: "Table name",
        sortable: :asc
      },
      %{
        field: :node,
        header: "Node"
      },
      %{
        field: :memory,
        header: "Memory"
      },
      %{
        field: :size,
        header: "Row count"
      },
      %{
        field: :indexes_memory,
        header: "Index memory"
      }
    ]
  end

  defp columns(:nodes) do
    [
      %{
        field: :node,
        header: "Node",
        sortable: :asc
      },
      %{
        field: :memory_total,
        header: "Memory total"
      },
      %{
        field: :memory_tables,
        header: "Memory tables"
      },
      %{
        field: :memory_derived_tables,
        header: "Memory derived tables"
      },
      %{
        field: :memory_indexes,
        header: "Memory indexes"
      },
      %{
        field: :table_count,
        header: "Table count"
      }
    ]
  end

  defp row_attrs(table, :nodes) do
    [
      {"phx-click", ""},
      {"phx-value-info", ""},
      {"phx-page-loading", true}
    ]
  end

  defp row_attrs(table, :changefeed_tables) do
    [
      {"phx-click", ""},
      {"phx-value-info", ""},
      {"phx-page-loading", true}
    ]
  end
end
