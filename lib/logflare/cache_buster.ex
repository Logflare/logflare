defmodule Logflare.CacheBuster do
  @moduledoc """
    Monitors our Postgres notifications and busts the cache accordingly.
  """

  use GenServer

  require Logger

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    Cainophile.Adapters.Postgres.subscribe(Logflare.PgPublisher, self())
    {:ok, state}
  end

  def handle_info(%Cainophile.Changes.Transaction{changes: changes}, state) do
    for record <- changes do
      handle_record(record)
    end

    {:noreply, state}
  end

  defp handle_record(%{relation: {"public", table = "sources"}, record: %{"token" => source_id}}) do
    Logger.info("CacheBuster notify: #{table} updated")
  end

  defp handle_record(%{relation: {"public", table}, record: _record}) do
    Logger.info("CacheBuster notify: #{table} updated")
  end
end
