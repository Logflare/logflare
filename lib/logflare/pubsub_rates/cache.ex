defmodule Logflare.PubSubRates.Cache do
  @moduledoc false
  require Logger

  alias Logflare.Source
  alias Logflare.Cluster

  @cache __MODULE__
  @default_bucket_width 60

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    %{id: __MODULE__, start: {Cachex, :start_link, [@cache, [stats: stats]]}}
  end

  def cache_rates(source_id, rates) when is_atom(source_id) do
    {:ok, val} = Cachex.get(__MODULE__, {source_id, "rates"})

    rates =
      if val,
        do: Map.merge(val, rates),
        else: rates

    cluster_rate = merge_node_rates(rates)

    rates = Map.put(rates, :cluster, cluster_rate)

    Cachex.put(__MODULE__, {source_id, "rates"}, rates, ttl: :timer.seconds(5))
  end

  def cache_inserts(source_id, inserts) when is_atom(source_id) do
    {:ok, val} = Cachex.get(__MODULE__, {source_id, "inserts"})

    inserts =
      if val,
        do: Map.merge(val, inserts),
        else: inserts

    Cachex.put(__MODULE__, {source_id, "inserts"}, inserts, ttl: :timer.seconds(5))
  end

  @doc """
  Stores a node map of buffer counts on the local cache.
  Merges a node map into the local cache.
  """
  @typep node_buffers :: %{atom() => non_neg_integer()}
  @spec cache_buffers(non_neg_integer(), non_neg_integer(), node_buffers()) :: {:ok, true}
  def cache_buffers(source_id, backend_id, buffers) when is_integer(source_id) do
    resolved =
      case get_buffers(source_id, backend_id) do
        {:ok, val} when val != nil -> Map.merge(val, buffers)
        _ -> buffers
      end

    Cachex.put(__MODULE__, {source_id, backend_id, "buffers"}, resolved, ttl: :timer.seconds(5))
  end

  @doc """
  Returns a node mapping of buffer lengths across the cluster.
  """
  @spec get_buffers(atom(), String.t() | nil) :: map()
  def get_buffers(source_token, backend_token) do
    Cachex.get(__MODULE__, {source_token, backend_token, "buffers"})
  end

  @doc """
  Returns the sum of all buffers across the cluster for a given source and backend combination.
  """
  @spec get_cluster_buffers(non_neg_integer()) :: non_neg_integer()
  @spec get_cluster_buffers(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def get_cluster_buffers(source_id, backend_id \\ nil) when is_integer(source_id) do
    case get_buffers(source_id, backend_id) do
      {:ok, node_buffers} when node_buffers != nil -> merge_buffers(node_buffers)
      _ -> 0
    end
  end

  def get_rates(source_id) when is_atom(source_id) do
    Cachex.get(__MODULE__, {source_id, "rates"})
  end

  def get_local_rates(source_id) when is_atom(source_id) do
    node = Node.self()

    default = %{
      average_rate: 0,
      last_rate: 0,
      max_rate: 0,
      limiter_metrics: %{average: 0, duration: @default_bucket_width, sum: 0}
    }

    case get_rates(source_id) do
      {:ok, nil} ->
        default

      {:ok, rates} ->
        Map.get(rates, node, default)

      {:error, :no_cache} ->
        default

        {:error, _} = err ->
        Logger.error("Error when getting pubsub cluster rates: #{inspect(err)}")

        %{
          average_rate: -1,
          last_rate: -1,
          max_rate: -1,
          limiter_metrics: %{average: 100_000, duration: @default_bucket_width, sum: 6_000_000}
        }
    end
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

      {:error, _} = err ->
        Logger.error("Error when getting pubsub clustr rates: #{inspect(err)}")

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
    nodes = Cluster.Utils.node_list_all()

    Map.take(node_buffers, nodes)
    |> Enum.map(fn {_, y} -> y.len end)
    |> Enum.sum()
  end

  defp merge_inserts(nodes_inserts) do
    nodes = Cluster.Utils.node_list_all()
    nodes_inserts = Map.take(nodes_inserts, nodes)

    nodes_total = Enum.map(nodes_inserts, fn {_node, y} -> y.node_inserts end) |> Enum.sum()

    bq_max =
      Enum.map(nodes_inserts, fn {_node, y} -> y.bq_inserts end) |> Enum.max(&>=/2, fn -> 0 end)

    nodes_total + bq_max
  end

  defp merge_node_rates(nodes_rates) do
    nodes = Cluster.Utils.node_list_all()
    nodes_rates = Map.take(nodes_rates, nodes)

    acc = {
      :nonode@nohost,
      %{
        average_rate: 0,
        last_rate: 0,
        limiter_metrics: %{average: 0, duration: @default_bucket_width, sum: 0},
        max_rate: 0
      }
    }

    {_, rates} =
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
