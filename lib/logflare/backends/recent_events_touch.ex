defmodule Logflare.Backends.RecentEventsTouch do
  @moduledoc """
  Periodically updates a Source.log_events_updated_at field to the latest ingested_at timestamp.

  Source.log_events_updated_at is used for determining sources to warm on node startup.
  """
  use TypedStruct
  use GenServer

  alias Logflare.Sources
  alias Logflare.Backends
  alias Logflare.Sources

  require Logger

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: Backends.via_source(args[:source], __MODULE__),
      hibernate_after: 5_000,
      spawn_opt: [
        fullsweep_after: 100
      ]
    )
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)
    touch_every = args[:touch_every] || Enum.random(10..30) * :timer.minutes(1)
    touch(touch_every)

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       source_token: source.token,
       source_id: source.id,
       touch_every: touch_every
     }}
  end

  def handle_info(:touch, %{source_id: source_id} = state) do
    source =
      source_id
      |> Sources.Cache.get_by_id()

    Backends.list_recent_logs_local(source)
    |> case do
      [] ->
        :noop

      [_ | _] = events ->
        prev = source.log_events_updated_at
        latest_ts = Enum.map(events, & &1.ingested_at) |> Enum.max(NaiveDateTime)

        cond do
          prev >= latest_ts ->
            :noop

          true ->
            source
            |> Sources.update_source(%{log_events_updated_at: latest_ts})
        end
    end

    touch(state.touch_every)
    {:noreply, state}
  end

  def handle_info({:EXIT, _from, _reason}, state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    Logger.warning("[#{__MODULE__}] Unhandled message: #{inspect(message)}")

    {:noreply, state}
  end

  def terminate(reason, _state) do
    Logger.info("[#{__MODULE__}] Going Down: #{inspect(reason)}")
    reason
  end

  defp touch(every) do
    Process.send_after(self(), :touch, every)
  end
end
