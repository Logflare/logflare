defmodule Logflare.Backends.IngestEventQueue.DemandWorker do
  @moduledoc """
  Fetches :pending events from the :ets queue and marks them as :ingested. for a given source-backend combination.

  Should be started under a SourceSup
  """
  use GenServer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Source
  alias Logflare.Backends.Backend
  alias Logflare.Backends
  require Logger

  def start_link(opts) do
    backend = Keyword.get(opts, :backend)
    bid = if backend, do: backend.id
    source = Keyword.get(opts, :source)
    IngestEventQueue.upsert_tid({source, backend})

    GenServer.start_link(__MODULE__, {source.id, bid},
      name: Backends.via_source(source.id, __MODULE__, bid)
    )
  end

  def init(sid_bid) do
    {:ok, sid_bid}
  end

  def fetch({%Source{id: sid}, %Backend{id: bid}}, n), do: fetch({sid, bid}, n)
  def fetch({%Source{id: sid}, nil}, n), do: fetch({sid, nil}, n)

  def fetch({_sid, _bid}, 0), do: {:ok, []}

  def fetch({sid, bid}, n) do
    Backends.via_source(sid, __MODULE__, bid)
    |> GenServer.call({:fetch, n})
  end

  def handle_call({:fetch, n}, _caller, {source_id, backend_id} = sid_bid) do
    # get the events

    events =
      case IngestEventQueue.take_pending(sid_bid, n) do
        {:error, :not_initialized} ->
          Logger.warning(
            "IngestEventQueue not initialized, could not fetch events. source_id: #{source_id}",
            backend_id: backend_id
          )

          []

        {:ok, []} ->
          []

        {:ok, events} ->
          {:ok, _} = IngestEventQueue.mark_ingested(sid_bid, events)
          events
      end

    {:reply, {:ok, events}, sid_bid}
  end
end
