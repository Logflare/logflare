defmodule Logflare.Tracker.Cache do
  alias Logflare.{Tracker, Source}
  import Cachex.Spec
  require Logger

  @cache __MODULE__

  def child_spec(_) do
    cachex_opts = []

    %{
      id: :cachex_tracker_cache,
      start: {Cachex, :start_link, [Tracker.Cache, cachex_opts]}
    }
  end

  def cache_cluster_inserts() do
    sources =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "inserts")
      |> Enum.map(fn {_x, y} ->
        Map.drop(y, [:phx_ref, :phx_ref_prev])
      end)
      |> Enum.reduce(fn x, acc ->
        Enum.into(x, acc, fn {x, %{bq_inserts: y, node_inserts: z}} ->
          {x, %{bq_inserts: y, node_inserts: z + z}}
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
