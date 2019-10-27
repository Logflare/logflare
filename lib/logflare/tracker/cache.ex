defmodule Logflare.Tracker.Cache do
  alias Logflare.{Tracker}
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

  # def cache_cluster_buffers(sources) do
  #   Stream.each(sources, fn x ->
  #     {:ok, source_id} = Ecto.UUID.load(x.source_id)

  #     cache_cluster_buffer(source_id)
  #   end)
  #   |> Stream.run()
  # end

  def cache_cluster_buffers() do
    buffers =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
      |> Enum.map(fn {x, y} ->
        Map.drop(y, [:phx_ref, :phx_ref_prev])
        |> Enum.map(fn {x, y} -> {x, y.buffer} end)
      end)
      |> Enum.reduce(fn x, acc ->
        Enum.into(x, acc, fn {x, y} -> {x, y + y} end)
      end)

    Enum.each(buffers, fn {x, y} -> Cachex.put(Tracker.Cache, String.to_atom(x), y) end)
  end

  # def cache_cluster_buffer(source_id) do
  #   buffer =
  #     Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
  #     # |> IO.inspect()
  #     |> Stream.map(fn {_node, sources} ->
  #       if x = sources[source_id], do: x.buffer, else: 0
  #     end)
  #     |> Enum.sum()

  #   Cachex.put(Tracker.Cache, String.to_atom(source_id), buffer)
  # end

  def get_cluster_buffer(source_id) do
    case Cachex.get(Tracker.Cache, source_id) do
      {:ok, buffer} ->
        buffer

      {:error, _} ->
        Logger.error("Tracker cache error!")
        0
    end
  end
end
