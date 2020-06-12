defmodule Logflare.Tracker.SourceNodeInserts do
  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.Tracker
  alias Logflare.Cluster

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_total_inserts_every 250

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
    init_trackers()
    update_tracker_every(0)
    check_total_inserts(0)
    {:ok, state}
  end

  def handle_info(:update_tracker, state) do
    query =
      from(s in "sources",
        select: %{
          source_id: s.token
        }
      )

    sources =
      Repo.all(query)
      |> Stream.map(fn x ->
        {:ok, source_id} = Ecto.UUID.load(x.source_id)
        source_id_atom = String.to_atom(source_id)
        node_inserts = Source.Data.get_node_inserts(source_id_atom)
        bq_inserts = Source.Data.get_bq_inserts(source_id_atom)

        {source_id, %{node_inserts: node_inserts, bq_inserts: bq_inserts}}
      end)
      |> Enum.into(%{})

    update_tracker(sources, "inserts")
    update_tracker_every()

    {:noreply, state}
  end

  def handle_info(:check_total_inserts, state) do
    Tracker.Cache.cache_cluster_inserts()

    check_total_inserts()
    {:noreply, state}
  end

  defp update_tracker(sources, type) do
    Logflare.Tracker.update(Logflare.Tracker, self(), type, Node.self(), sources)
  end

  defp init_trackers() do
    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "inserts",
      Node.self(),
      %{}
    )
  end

  defp update_tracker_every(delay \\ @check_total_inserts_every) do
    Process.send_after(self(), :update_tracker, delay * Cluster.Utils.actual_cluster_size())
  end

  defp check_total_inserts(delay \\ @check_total_inserts_every) do
    Process.send_after(self(), :check_total_inserts, delay)
  end
end
