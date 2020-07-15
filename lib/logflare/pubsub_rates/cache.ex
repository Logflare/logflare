defmodule Logflare.PubSubRates.Cache do
  require Logger

  @default_bucket_width 60

  def child_spec(_) do
    cachex_opts = []

    %{
      id: __MODULE__,
      start: {Cachex, :start_link, [__MODULE__, cachex_opts]}
    }
  end

  def get_rates(source_id) do
    Cachex.get(__MODULE__, source_id)
  end

  def get_cluster_rates(source_id) do
    case get_rates(source_id) do
      {:ok, nil} ->
        %{
          average_rate: 0,
          last_rate: 0,
          max_rate: 0,
          limiter_metrics: %{average: 0, duration: @default_bucket_width, sum: 0}
        }

      {:ok, rates} ->
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

  def cache_rates(source_id, rates) do
    Cachex.get_and_update(__MODULE__, source_id, fn
      nil -> {:commit, rates}
      val -> {:commit, Map.merge(val, rates)}
    end)
  end

  def merge_node_rates(nodes_rates) do
    acc = {
      :nonode@nohost,
      %{
        average_rate: 0,
        last_rate: 0,
        limiter_metrics: %{average: 0, duration: 60, sum: 0},
        max_rate: 0
      }
    }

    {:cluster, rates} =
      Enum.reduce(nodes_rates, acc, fn {_, x}, {_, acc} ->
        map =
          Map.merge(x, acc, fn kk, vv1, vv2 ->
            case kk do
              :average_rate ->
                vv1 + vv2

              :last_rate ->
                vv1 + vv2

              :max_rate ->
                vv1 + vv2

              :limiter_metrics ->
                Map.merge(vv1, vv2, fn kkk, vvv1, vvv2 ->
                  case kkk do
                    :average -> vvv1 + vvv2
                    :duration -> @default_bucket_width
                    :sum -> vvv1 + vvv2
                  end
                end)
            end
          end)

        {:cluster, map}
      end)

    rates
  end
end
