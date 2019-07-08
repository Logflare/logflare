defmodule LogflareWeb.Source.SearchLV do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query}
  alias Logflare.{Sources, Source, Logs, LogEvent}
  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.SearchOpts
  alias LogflareWeb.SourceView
  use Phoenix.LiveView
  use TypedStruct

  def render(assigns) do
    Phoenix.View.render(SourceView, "search_frame.html", assigns)
  end

  def mount(%{source: source}, socket) do
    {:ok,
     assign(socket,
       query: nil,
       loading: false,
       log_events: [],
       tailing?: :initial,
       source: source,
       task: nil,
       partitions: nil
     )}
  end

  def handle_event("search", params, socket) do
    %{"search" => %{"q" => query}} = params

    %{query: prev_query, task: task, tailing?: tailing?, log_events: log_events} = socket.assigns
    new_query? = query != prev_query || not tailing?

    task =
      if new_query? && task do
        Task.shutdown(task, 100)
        nil
      else
        task
      end
    log_events = if new_query?, do: [], else: log_events

    send(self(), :search)

    {:noreply,
     assign(socket,
       query: query,
       result: "Searching...",
       loading: new_query?,
       log_events: log_events,
       task: task
     )}
  end

  def handle_event("toggle_tailing", _, socket) do
    tailing? = if socket.assigns.tailing?, do: false, else: :initial
    if tailing?, do: send(self(), :search)

    socket = assign(socket, tailing?: tailing?)

    {:noreply, socket}
  end

  def default_partitions() do
    today = Date.utc_today() |> Timex.to_datetime("Etc/UTC")
    {today, today}
  end

  def handle_info(:search, socket = %{assigns: assigns}) do
    %{task: task, tailing?: tailing?} = assigns

    task =
      if task do
        task
      else
        SearchOpts
        |> struct(socket.assigns)
        |> run_search_task()
      end

    tailing? = if tailing? == :initial, do: true, else: tailing?

    {:noreply, assign(socket, tailing?: tailing?, task: task)}
  end

  def handle_info({_ref, {:search_results, log_events}}, socket) do
    log_events =
      log_events
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: socket.assigns.source}))
      |> Enum.concat(socket.assigns.log_events)
      |> Enum.sort_by(& &1.body.timestamp, &>=/2)
      |> Enum.take(10)

    if socket.assigns.tailing?, do: Process.send_after(self(), :search, 1_000)

    {:noreply, assign(socket, log_events: log_events, task: nil, loading: false)}
  end

  # handles {:DOWN, ... } msgs from task
  def handle_info(_, state), do: {:noreply, state}

  def run_search_task(search_opts) do
    Task.async(fn ->
      {:ok, %{rows: log_events}} = Search.search(search_opts)
      {:search_results, log_events}
    end)
  end
end
