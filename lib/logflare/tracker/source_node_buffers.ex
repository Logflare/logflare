defmodule Logflare.Tracker.SourceNodeBuffers do
  alias Logflare.Sources
  alias Logflare.Repo
  alias Logflare.Tracker

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_buffers_every 1_000

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
    init_trackers()
    check_buffers(0)

    {:ok, state}
  end

  def handle_info(:check_buffers, state) do
    query =
      from(s in "sources",
        select: %{
          source_id: s.token
        }
      )

    sources = Repo.all(query)

    sources_with_buffer =
      Stream.map(sources, fn x ->
        {:ok, source_id} = Ecto.UUID.load(x.source_id)
        buffer = Sources.Buffers.dirty_len(String.to_atom(source_id))

        {source_id, %{buffer: buffer}}
      end)
      |> Enum.into(%{})

    update_tracker(sources_with_buffer, "buffers")
    Tracker.Cache.cache_cluster_buffers()

    check_buffers()
    {:noreply, state}
  end

  defp update_tracker(sources, type) do
    Logflare.Tracker.update(Logflare.Tracker, self(), type, Node.self(), sources)
  end

  defp init_trackers() do
    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "buffers",
      Node.self(),
      %{}
    )
  end

  defp check_buffers(delay \\ @check_buffers_every) do
    Process.send_after(self(), :check_buffers, delay)
  end
end
