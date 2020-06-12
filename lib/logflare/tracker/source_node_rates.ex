defmodule Logflare.Tracker.SourceNodeRates do
  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.Tracker
  alias Logflare.Cluster

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_rates_every 250

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
    check_rates(0)
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
    update_tracker_every()

    {:noreply, state}
  end

  def handle_info(:check_rates, state) do
    Tracker.Cache.cache_cluster_rates()

    check_rates()
    {:noreply, state}
  end

  defp update_tracker(sources, type) do
    Logflare.Tracker.update(Logflare.Tracker, self(), type, Node.self(), sources)
  end

  defp init_trackers() do
    Logflare.Tracker.track(
      Logflare.Tracker,
      self(),
      "rates",
      Node.self(),
      %{}
    )
  end

  defp check_rates(delay \\ @check_rates_every) do
    Process.send_after(self(), :check_rates, delay)
  end

  defp update_tracker_every(delay \\ @check_rates_every) do
    Process.send_after(self(), :update_tracker, delay * Cluster.Utils.actual_cluster_size())
  end
end
