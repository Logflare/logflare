defmodule Logflare.Tracker.Cache do
  alias Logflare.{Tracker, Source}
  import Cachex.Spec
  require Logger

  @ttl 5_000

  def child_spec(_) do
    cachex_opts = [
      expiration: expiration(default: @ttl)
    ]

    %{
      id: :cachex_tracker_cache,
      start: {Cachex, :start_link, [Tracker.Cache, cachex_opts]}
    }
  end

  def cache_cluster_rates() do
    sources =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "rates")
      |> Enum.map(fn {_x, y} ->
        Map.drop(y, [:phx_ref, :phx_ref_prev])
        |> Enum.into(%{})
      end)
      |> Enum.reduce(fn x, acc ->
        Map.merge(x, acc, fn _k, v1, v2 ->
          Map.merge(v1, v2, fn kk, vv1, vv2 ->
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
                    :duration -> 60
                    :sum -> vvv1 + vvv2
                  end
                end)
            end
          end)
        end)
      end)

    Enum.each(sources, fn {x, y} ->
      Cachex.put(Tracker.Cache, Source.RateCounterServer.name(x), y)
    end)
  end

  def get_cluster_rates(source_id) when is_atom(source_id) do
    case Cachex.get(Tracker.Cache, Source.RateCounterServer.name(source_id)) do
      {:ok, inserts} ->
        inserts

      {:error, _} ->
        Logger.error("Tracker cache error!")

        %{
          average_rate: 0,
          last_rate: 0,
          max_rate: 0,
          limiter_metrics: %{average: 0, duration: 60, sum: 0}
        }
    end
  end

  def cache_cluster_inserts() do
    sources =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "inserts")
      |> Enum.map(fn {_x, y} ->
        Map.drop(y, [:phx_ref, :phx_ref_prev])
        |> Enum.into(%{})
      end)
      |> Enum.reduce(fn x, acc ->
        Map.merge(x, acc, fn _k, v1, v2 ->
          Map.merge(v1, v2, fn kk, vv1, vv2 ->
            case kk do
              :node_inserts -> vv1 + vv2
              :bq_inserts -> Enum.max([vv1, vv2])
              _ -> vv1
            end
          end)
        end)
      end)

    Enum.each(sources, fn {x, %{bq_inserts: y, node_inserts: z}} ->
      Cachex.put(Tracker.Cache, String.to_atom(x), y + z)
    end)
  end

  def get_cluster_inserts(source_id) when is_atom(source_id) do
    case Cachex.get(Tracker.Cache, source_id) do
      {:ok, inserts} ->
        inserts

      {:error, _} ->
        Logger.error("Tracker cache error!")
        0
    end
  end

  def cache_cluster_buffers() do
    sources =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
      |> Enum.map(fn {_x, y} ->
        Map.drop(y, [:phx_ref, :phx_ref_prev])
        |> Enum.map(fn {x, y} -> {x, y.buffer} end)
        |> Enum.into(%{})
      end)
      |> Enum.reduce(fn x, acc ->
        Map.merge(x, acc, fn _k, v1, v2 -> v1 + v2 end)
      end)

    Enum.each(sources, fn {x, y} ->
      Cachex.put(Tracker.Cache, Source.BigQuery.Buffer.name(x), y)
    end)
  end

  def get_cluster_buffer(source_id) when is_atom(source_id) do
    case Cachex.get(Tracker.Cache, Source.BigQuery.Buffer.name(source_id)) do
      {:ok, buffer} ->
        buffer

      {:error, _} ->
        Logger.error("Tracker cache error!")
        0
    end
  end
end
