defmodule Logflare.Tracker.Cache do
  alias Logflare.{Tracker, Source}
  import Cachex.Spec
  require Logger

  @ttl 5_000
  @default_bucket_width 60

  def child_spec(_) do
    cachex_opts = [
      expiration: expiration(default: @ttl)
    ]

    %{
      id: :cachex_tracker_cache,
      start: {Cachex, :start_link, [Tracker.Cache, cachex_opts]}
    }
  end

  def cache_cluster_buffers() do
    Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
    |> Stream.map(fn {_x, y} ->
      Map.drop(y, [:phx_ref, :phx_ref_prev])
      |> Stream.map(fn {x, y} -> {x, y.buffer} end)
      |> Enum.into(%{})
    end)
    |> Enum.reduce(fn x, acc ->
      Map.merge(x, acc, fn _k, v1, v2 -> v1 + v2 end)
    end)
    |> Stream.each(fn {x, y} ->
      Cachex.put(Tracker.Cache, Source.BigQuery.Buffer.name(x), y)
    end)
    |> Stream.run()
  end

  def get_cluster_buffer(source_id) when is_atom(source_id) do
    case Cachex.get(Tracker.Cache, Source.BigQuery.Buffer.name(source_id)) do
      {:ok, nil} ->
        Logger.error("Tracker buffer cache expired!", source_id: source_id)
        0

      {:ok, buffer} ->
        buffer

      {:error, _} ->
        0
    end
  end
end
