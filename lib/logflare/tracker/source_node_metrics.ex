defmodule Logflare.Tracker.SourceNodeMetrics do
  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.Tracker

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_buffers_every 1_000
  @check_rates_every 1_000
  @check_total_inserts_every 1_000

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
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

    sources = Repo.all(query)

    sources_with_buffer =
      Stream.map(sources, fn x ->
        {:ok, source_id} = Ecto.UUID.load(x.source_id)
        buffer = Source.Data.get_buffer(source_id)

        {source_id, %{buffer: buffer}}
      end)
      |> Enum.into(%{})

    update_tracker(sources_with_buffer, "buffers")
    Tracker.Cache.cache_cluster_buffers()

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
      |> Stream.map(fn x ->
        {:ok, source_id} = Ecto.UUID.load(x.source_id)
        source_id_atom = String.to_atom(source_id)
        node_inserts = Source.Data.get_node_inserts(source_id_atom)
        bq_inserts = Source.Data.get_bq_inserts(source_id_atom)

        {source_id, %{node_inserts: node_inserts, bq_inserts: bq_inserts}}
      end)
      |> Enum.into(%{})

    update_tracker(sources, "inserts")
    Tracker.Cache.cache_cluster_inserts()

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
      |> Stream.map(fn x ->
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
    #   rates = Logflare.Tracker.dirty_list(Logflare.Tracker, "rates")

    #   max_rate =
    #     rates
    #     |> Enum.map(fn {_node, sources} ->
    #       if x = sources[Atom.to_string(source_id)], do: x.max_rate, else: 0
    #     end)
    #     |> Enum.sum()

    #   avg_rate =
    #     rates
    #     |> Enum.map(fn {_node, sources} ->
    #       if x = sources[Atom.to_string(source_id)], do: x.average_rate, else: 0
    #     end)
    #     |> Enum.sum()

    #   last_rate =
    #     rates
    #     |> Enum.map(fn {_node, sources} ->
    #       if x = sources[Atom.to_string(source_id)], do: x.last_rate, else: 0
    #     end)
    #     |> Enum.sum()

    #   average =
    #     rates
    #     |> Enum.map(fn {_node, sources} ->
    #       if x = sources[Atom.to_string(source_id)], do: x.limiter_metrics.average, else: 0
    #     end)
    #     |> Enum.sum()

    #   duration = 60

    #   sum =
    #     rates
    #     |> Enum.map(fn {_node, sources} ->
    #       if x = sources[Atom.to_string(source_id)], do: x.limiter_metrics.sum, else: 0
    #     end)
    #     |> Enum.sum()

    #   %{
    #     average_rate: avg_rate,
    #     last_rate: last_rate,
    #     max_rate: max_rate,
    #     limiter_metrics: %{average: average, duration: duration, sum: sum}
    #   }

    %{
      average_rate: 0,
      last_rate: 0,
      max_rate: 0,
      limiter_metrics: %{average: 0, duration: 60, sum: 0}
    }
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
