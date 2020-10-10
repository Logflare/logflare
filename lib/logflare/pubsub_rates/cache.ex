defmodule Logflare.PubSubRates.Cache do
  require Logger

  alias Logflare.Source

  @default_bucket_width 60

  def child_spec(_) do
    cachex_opts = []

    %{
      id: __MODULE__,
      start: {Cachex, :start_link, [__MODULE__, cachex_opts]}
    }
  end

  def cache_rates(source_id, rates) do
    Cachex.get_and_update(__MODULE__, {source_id, "rates"}, fn
      nil -> {:commit, rates}
      val -> {:commit, Map.merge(val, rates)}
    end)

    Cachex.expire(__MODULE__, {source_id, "rates"}, :timer.seconds(5))
  end

  def cache_inserts(source_id, inserts) do
    Cachex.get_and_update(__MODULE__, {source_id, "inserts"}, fn
      nil -> {:commit, inserts}
      val -> {:commit, Map.merge(val, inserts)}
    end)
  end

  def cache_buffers(source_id, buffers) do
    Cachex.get_and_update(__MODULE__, {source_id, "buffers"}, fn
      nil -> {:commit, buffers}
      val -> {:commit, Map.merge(val, buffers)}
    end)
  end

  def get_buffers(source_id) do
    Cachex.get(__MODULE__, {source_id, "buffers"})
  end

  def get_cluster_buffers(source_id) do
    case get_buffers(source_id) do
      {:ok, nil} ->
        0

      {:ok, node_buffers} ->
        merge_buffers(node_buffers)

      {:error, _} ->
        0
    end
  end

  def get_rates(source_id) do
    Cachex.get(__MODULE__, {source_id, "rates"})
  end

  def get_cluster_rates(source_id) do
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
        # cache this function calc so we don't do it on every request
        merge_node_rates(rates)

      {:error, _} ->
        %{
          average_rate: -1,
          last_rate: -1,
          max_rate: -1,
          limiter_metrics: %{average: 100_000, duration: @default_bucket_width, sum: 6_000_000}
        }
    end
  end

  def get_inserts(source_id) do
    Cachex.get(__MODULE__, {source_id, "inserts"})
  end

  def get_cluster_inserts(source_id) do
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
