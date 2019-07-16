defmodule LogflareWeb.Source.TailSearchLV do
  use Phoenix.LiveView
  alias LogflareWeb.SourceView

  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.SearchOperation, as: SO
  alias Logflare.LogEvent

  @tailing_search_interval 30_000

  use PrintDecorator

  @decorate_all print()

  def render(assigns) do
    Phoenix.View.render(SourceView, "logs_search.html", assigns)
  end

  def mount(%{source: source, user: user}, socket) do
    send(self(), :search)

    {:ok,
     assign(socket,
       querystring: nil,
       task: nil,
       log_events: [],
       search_op: nil,
       tailing?: true,
       tailing_initial?: true,
       source: source,
       user: user
     )}
  end

  def handle_event("search", %{"search" => search} = _params, socket) do
    assigns = socket.assigns
    maybe_new_querystring = search["querystring"]

    new_query? = assigns.querystring != maybe_new_querystring

    socket =
      case {new_query?, assigns.tailing?, assigns.task} do
        {false, true, %Task{}} ->
          socket

        {true, true, %Task{}} ->
          Task.shutdown(assigns.task, :brutal_kill)

          reset_and_start_search_task(socket,
            querystring: maybe_new_querystring,
            tailing_initial?: true
          )

        {true, false, %Task{}} ->
          Task.shutdown(assigns.task, :brutal_kill)

          reset_and_start_search_task(socket, querystring: maybe_new_querystring)

        {_, _, nil} ->
          reset_and_start_search_task(socket, querystring: maybe_new_querystring)
      end

    {:noreply, socket}
  end

  def handle_event("toggle_tailing", _, socket) do
    now_tailing? = not socket.assigns.tailing?
    now_tailing_initial? = if now_tailing?, do: true, else: false
    task = socket.assigns.task

    if now_tailing?, do: send(self(), :search)
    if not now_tailing? && task, do: Task.shutdown(task, :brutal_kill)

    {:noreply,
     assign(socket, tailing?: now_tailing?, tailing_initial?: now_tailing_initial?, task: nil)}
  end

  def reset_and_start_search_task(socket, kw) do
    kw = Keyword.merge(kw, log_events: [])

    socket
    |> assign(kw)
    |> start_search_task()
  end

  def handle_info(:search, socket) do
    start_search_task(socket)
    {:noreply, socket}
  end

  def handle_info({_ref, {:search_results, %SO{} = search_opn}}, socket) do
    log_events =
      search_opn
      |> Map.get(:rows)
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: socket.assigns.source}))
      |> Enum.concat(socket.assigns.log_events)
      |> Enum.sort_by(& &1.body.timestamp, &>=/2)
      |> Enum.take(100)

    socket = assign(socket, tailing_initial?: false)
    if socket.assigns.tailing?, do: Process.send_after(self(), :search, @tailing_search_interval)

    {:noreply, assign(socket, log_events: log_events, task: nil, search_op: search_opn)}
  end

  # handles {:DOWN, ... } msgs from task
  def handle_info(_, state), do: {:noreply, state}

  def start_search_task(%{assigns: %{querystring: nil}}), do: :noop

  def start_search_task(socket) do
    task =
      Task.async(fn ->
        {:ok, search_opn} =
          SO
          |> struct(socket.assigns)
          |> Search.search()

        {:search_results, search_opn}
      end)

    assign(socket, task: task)
  end
end
