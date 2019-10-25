defmodule Logflare.Tracker.SourceNodeMetrics do
  alias Logflare.Source
  alias Logflare.Repo

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_buffers_every 1_000
  @check_rates_every 1_000
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
    check_rates()
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

  def handle_info(:check_rates, state) do
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
        avg_rate = Source.Data.get_avg_rate(source_id_atom)
        last_rate = Source.Data.get_rate(source_id_atom)
        max_rate = Source.Data.get_max_rate(source_id_atom)

        limiter_metrics = Source.RateCounterServer.get_rate_metrics(source_id_atom)

        {source_id,
         %{
           average_rate: avg_rate,
           last_rate: last_rate,
           max_rate: max_rate,
           limiter_metrics: limiter_metrics
         }}
      end)
      |> Enum.into(%{})

    update_tracker(sources, "rates")

    check_rates()
    {:noreply, state}
  end

  def get_cluster_rates(source_id) do
    rates = Logflare.Tracker.dirty_list(Logflare.Tracker, "rates")

    max_rate =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].max_rate end)
      |> Enum.sum()

    avg_rate =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].average_rate end)
      |> Enum.sum()

    last_rate =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].last_rate end)
      |> Enum.sum()

    average =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].limiter_metrics.average end)
      |> Enum.sum()

    duration =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].limiter_metrics.duration end)
      |> Enum.fetch!(0)

    sum =
      rates
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].limiter_metrics.sum end)
      |> Enum.sum()

    %{
      average_rate: avg_rate,
      last_rate: last_rate,
      max_rate: max_rate,
      limiter_metrics: %{average: average, duration: duration, sum: sum}
    }
  end

  def get_cluster_buffer(source_id) do
    Logflare.Tracker.dirty_list(Logflare.Tracker, "buffers")
    |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].buffer end)
    |> Enum.sum()
  end

  def get_cluster_inserts(source_id) do
    inserts = Logflare.Tracker.dirty_list(Logflare.Tracker, "inserts")

    cluster_inserts =
      inserts
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].node_inserts end)
      |> Enum.sum()

    bq_inserts_max =
      inserts
      |> Enum.map(fn {_node, data} -> data[Atom.to_string(source_id)].bq_inserts end)
      |> Enum.max(fn -> 0 end)

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

    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "rates",
      Node.self(),
      %{}
    )
  end

  defp check_buffers() do
    Process.send_after(self(), :check_buffers, @check_buffers_every)
  end

  defp check_rates() do
    Process.send_after(self(), :check_rates, @check_rates_every)
  end

  defp check_total_inserts() do
    Process.send_after(self(), :check_total_inserts, @check_total_inserts_every)
  end
end
