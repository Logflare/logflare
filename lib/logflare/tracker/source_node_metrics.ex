defmodule Logflare.Tracker.SourceNodeMetrics do
  alias Logflare.Source
  alias Logflare.Repo

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
    Process.flag(:trap_exit, true)

    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "buffers",
      Node.self(),
      %{}
    )

    check_buffers()
    {:ok, state}
  end

  def handle_info(:check_buffers, state) do
    query =
      from(s in "sources",
        select: %{
          source_id: s.token
        }
      )

    sources =
      Repo.all(query)
      |> Enum.map(fn x ->
        {:ok, source_id} = Ecto.UUID.load(x.source_id)
        buffer = Source.Data.get_buffer(source_id)

        {source_id, %{buffer: buffer}}
      end)
      |> Enum.into(%{})

    update_tracker(sources)

    check_buffers()
    {:noreply, state}
  end

  def get_cluster_buffer(source_id) do
    payload =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
      |> Enum.map(fn {node, data} -> data[Atom.to_string(source_id)].buffer end)
      |> Enum.sum()
  end

  defp update_tracker(sources) do
    Logflare.Tracker.update(Logflare.Tracker, self(), "buffers", Node.self(), sources)
  end

  defp check_buffers() do
    Process.send_after(self(), :check_buffers, @check_buffers_every)
  end
end
