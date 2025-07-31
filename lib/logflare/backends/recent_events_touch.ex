defmodule Logflare.Backends.RecentEventsTouch do
  @moduledoc """
  Periodically updates a Source.log_events_updated_at field to the latest ingested_at timestamp.

  Source.log_events_updated_at is used for determining sources to warm on node startup.
  """
  use TypedStruct
  use GenServer, restart: :transient

  alias Logflare.Sources
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Utils
  require Logger

  ## Server
  def start_link(args) do
    GenServer.start_link(__MODULE__, args,
      name: name(args[:source]),
      hibernate_after: 5_000
    )
  end

  def name(source) do
    Backends.via_source(source, __MODULE__)
  end

  ## Client
  def init(args) do
    source = Keyword.get(args, :source)

    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: source.token, source_token: source.token)
    touch_every = args[:touch_every]
    touch(touch_every)

    Logger.debug("[#{__MODULE__}] Started")

    {:ok,
     %{
       source_token: source.token,
       source_id: source.id,
       touch_every: touch_every
     }}
  end

  defp random_interval_ms() do
    min = :timer.minutes(30)
    max = :timer.minutes(120)
    Enum.random(min..max)
  end

  def handle_info(:touch, %{source_id: source_id} = state) do
    # use a Task to separate out heap memory for any bound variables
    Utils.Tasks.start_child(fn -> do_work(source_id) end)
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

  defp do_work(source_id) do
    with %_{log_events_updated_at: prev} = source <- Sources.Cache.get_by_id(source_id) do
      source
      |> Backends.list_recent_logs_local()
      |> Enum.map(& &1.ingested_at)
      |> Enum.max(NaiveDateTime, fn -> nil end)
      |> then(fn
        latest_ts when latest_ts != nil and prev < latest_ts ->
          Sources.update_source(source, %{log_events_updated_at: latest_ts})

        _ ->
          :noop
      end)
    else
      _ -> :noop
    end
  end

  defp touch(every) do
    Process.send_after(self(), :touch, every || random_interval_ms())
  end
end
