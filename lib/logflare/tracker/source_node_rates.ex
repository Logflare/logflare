defmodule Logflare.Tracker.SourceNodeRates do
  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.Tracker

  import Ecto.Query, only: [from: 2]

  require Logger

  use GenServer

  @check_rates_every 1_000

  def start_link() do
    GenServer.start_link(
      __MODULE__,
      [],
      name: __MODULE__
    )
  end

  def init(state) do
    init_trackers()
    check_rates()
    {:ok, state}
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

  defp check_rates() do
    Process.send_after(self(), :check_rates, @check_rates_every)
  end
end
