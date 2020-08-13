defmodule Logflare.SystemMetrics.Hackney.Poller do
  @moduledoc """
  Polls hackney stats.
  """

  use GenServer

  require Logger

  @poll_every 30_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    poll_metrics()
    {:ok, state}
  end

  def handle_info(:poll_metrics, state) do
    if Application.get_env(:logflare, :env) == :prod do
      bq_stats = :hackney_pool.get_stats(Client.BigQuery)
      default_stats = :hackney_pool.get_stats(:default)

      Logger.info("Hackney BigQuery stats!",
        hackney_stats: bq_stats,
        hackney_pool: Client.BigQuery
      )

      Logger.info("Hackney :default stats!", hackney_stats: default_stats, hackney_pool: :default)
    end

    poll_metrics()
    {:noreply, state}
  end

  defp poll_metrics() do
    Process.send_after(self(), :poll_metrics, @poll_every)
  end
end
