defmodule Logflare.Tracker.SourceNodeMetrics do
  alias Logflare.Source
  alias Logflare.Repo

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_buffers_every 1_000
  @check_total_inserts_every 250

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)
    init_trackers()
    check_buffers()
    check_total_inserts()
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

    update_tracker(sources, "buffers")

    check_buffers()
    {:noreply, state}
  end

  def handle_info(:check_total_inserts, state) do
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
        source_id_atom = String.to_atom(source_id)
        node_inserts = Source.Data.get_node_inserts(source_id_atom)
        bq_inserts = Source.Data.get_bq_inserts(source_id_atom)

        {source_id, %{node_inserts: node_inserts, bq_inserts: bq_inserts}}
      end)
      |> Enum.into(%{})

    update_tracker(sources, "inserts")

    check_total_inserts()
    {:noreply, state}
  end

  def get_cluster_buffer(source_id) do
    Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
    |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].buffer end)
    |> Enum.sum()
  end

  def get_cluster_inserts(source_id) do
    cluster_inserts =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "inserts")
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].node_inserts end)
      |> Enum.sum()

    bq_inserts_max =
      Logflare.Tracker.dirty_list(Logflare.Tracker, "inserts")
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].bq_inserts end)
      |> Enum.max()

    cluster_inserts + bq_inserts_max
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

    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "inserts",
      Node.self(),
      %{}
    )
  end

  defp check_buffers() do
    Process.send_after(self(), :check_buffers, @check_buffers_every)
  end

  defp check_total_inserts() do
    Process.send_after(self(), :check_total_inserts, @check_total_inserts_every)
  end
end
