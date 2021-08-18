defmodule Logflare.PubSubRates.Cache do
  require Logger

  alias Logflare.Source
  alias Logflare.Cluster

  @cache __MODULE__
  @default_bucket_width 60

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [@cache, [stats: true]]}}
  end

  def cache_rates(source_id, rates) when is_atom(source_id) do
    {:ok, val} = Cachex.get(__MODULE__, {source_id, "rates"})

    rates =
      if val,
        do: Map.merge(val, rates),
        else: rates

    cluster_rate = Map.take(rates, Cluster.Utils.node_list_all()) |> merge_node_rates()

    rates = Map.put(rates, :cluster, cluster_rate)

    Cachex.put(__MODULE__, {source_id, "rates"}, rates, ttl: :timer.seconds(5))
  end

  def cache_inserts(source_id, inserts) when is_atom(source_id) do
    {:ok, val} = Cachex.get(__MODULE__, {source_id, "inserts"})

    inserts =
      if val,
        do: Map.merge(val, inserts),
        else: inserts

    Cachex.put(__MODULE__, {source_id, "inserts"}, inserts)
  end

  def cache_buffers(source_id, buffers) when is_atom(source_id) do
    {:ok, val} = Cachex.get(__MODULE__, {source_id, "buffers"})

    buffers =
      if val,
        do: Map.merge(val, buffers),
        else: buffers

    Cachex.put(__MODULE__, {source_id, "buffers"}, buffers)
  end

  def get_buffers(source_id) when is_atom(source_id) do
    Cachex.get(__MODULE__, {source_id, "buffers"})
  end

  def get_cluster_buffers(source_id) when is_atom(source_id) do
    case get_buffers(source_id) do
      {:ok, nil} ->
        0

      {:ok, node_buffers} ->
        merge_buffers(node_buffers)

      {:error, _} ->
        0
    end
  end

  def get_rates(source_id) when is_atom(source_id) do
    Cachex.get(__MODULE__, {source_id, "rates"})
  end

  def get_cluster_rates(source_id) when is_atom(source_id) do
    case get_rates(source_id) do
      {:ok, nil} ->
        # This should be data from the node RCS * the cluster size, otherwise we're effectively not rate limiting until cluster rates get cached.
        %{
          average_rate: 0,
          last_rate: 0,
          max_rate: 0,
          limiter_metrics: %{average: 0, duration: @default_bucket_width, sum: 0}
        }

      {:ok, rates} ->
        Map.get(rates, :cluster)

      {:error, _} ->
        %{
          average_rate: -1,
          last_rate: -1,
          max_rate: -1,
          limiter_metrics: %{average: 100_000, duration: @default_bucket_width, sum: 6_000_000}
        }
    end
  end

  def get_inserts(source_id) when is_atom(source_id) do
    Cachex.get(__MODULE__, {source_id, "inserts"})
  end

  def get_cluster_inserts(source_id) when is_atom(source_id) do
    case get_inserts(source_id) do
      {:ok, nil} ->
        Source.Data.get_total_inserts(source_id)

      {:ok, inserts} ->
        merge_inserts(inserts)

      {:error, _} ->
        0
    end
  end

  def merge_buffers(node_buffers) do
    Enum.map(node_buffers, fn {_node, y} -> y.len end) |> Enum.sum()
  end

  defp merge_inserts(nodes_inserts) do
    nodes_total = Enum.map(nodes_inserts, fn {_node, y} -> y.node_inserts end) |> Enum.sum()
    bq_max = Enum.map(nodes_inserts, fn {_node, y} -> y.bq_inserts end) |> Enum.max()

    nodes_total + bq_max
  end

  defp merge_node_rates(nodes_rates) do
    acc = {
      :nonode@nohost,
      %{
        average_rate: 0,
        last_rate: 0,
        limiter_metrics: %{average: 0, duration: @default_bucket_width, sum: 0},
        max_rate: 0
      }
    }

    {:cluster, rates} =
      Enum.reduce(nodes_rates, acc, fn {_, x}, {_, acc} ->
        ar = x.average_rate + acc.average_rate
        lr = x.last_rate + acc.last_rate
        sum = x.limiter_metrics.sum + acc.limiter_metrics.sum
        mr = x.max_rate + acc.max_rate

        map = %{
          average_rate: ar,
          last_rate: lr,
          limiter_metrics: %{average: ar, duration: @default_bucket_width, sum: sum},
          max_rate: mr
        }

        {:cluster, map}
      end)

    rates
  end
end
