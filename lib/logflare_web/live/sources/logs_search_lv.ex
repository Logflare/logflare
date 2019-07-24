defmodule LogflareWeb.Source.TailSearchLV do
  use Phoenix.LiveView
  alias LogflareWeb.SourceView

  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.SearchOperation, as: SO
  alias Logflare.LogEvent

  @tailing_search_interval 10_000
  @user_idle_interval 60_000

  def render(assigns) do
    Phoenix.View.render(SourceView, "logs_search.html", assigns)
  end

  def mount(%{source: source, user: user, querystring: qs} = session, socket) do
    send(self(), :search)

    tailing =
      if is_nil(session[:tailing?]) do
        true
      else
        session[:tailing?]
      end

    {:ok,
     assign(socket,
       querystring: qs,
       task: nil,
       flash: %{},
       log_events: [],
       search_op: nil,
       tailing?: tailing,
       tailing_initial?: tailing,
       source: source,
       user: user,
       user_idle_interval: @user_idle_interval
     )}
  end

  def handle_event("search", %{"search" => search} = _params, socket) do
    assigns = socket.assigns
    maybe_new_querystring = search["querystring"]

    new_query? = assigns.querystring != maybe_new_querystring

    socket =
      case {new_query?, assigns.tailing?, assigns.task} do
        {false, true, %Task{}} ->
          # doesn't start a new tailing task if previous hasn't completed yet
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

    socket =
      socket
      |> assign(:flash, %{})
      |> maybe_put_flash()

    {:noreply, socket}
  end

  def handle_event("toggle_tailing", _, socket) do
    now_tailing? = not socket.assigns.tailing?
    now_tailing_initial? = if now_tailing?, do: true, else: false
    task = socket.assigns.task

    if now_tailing?, do: send(self(), :search)
    if not now_tailing? && task, do: Task.shutdown(task, :brutal_kill)

    socket =
      socket
      |> assign(
        tailing?: now_tailing?,
        tailing_initial?: now_tailing_initial?,
        task: nil
      )

    {:noreply, socket}
  end

  def handle_event("user_idle", _, socket) do
    socket =
      if socket.assigns.tailing? do
        flash = Map.put(%{}, :info, "Live search paused due to user inactivity")

        assign(
          socket,
          tailing?: false,
          tailing_initial?: false,
          flash: flash
        )
      else
        socket
      end

    {:noreply, socket}
  end

  def maybe_put_flash(%{assigns: as} = socket) do
    if String.contains?(as.querystring, "timestamp") and as.tailing? do
      assign(
        socket,
        flash:
          put_in(
            socket.assigns.flash,
            [:info],
            "Timestamp filter is ignored when live tailing is active"
          )
      )
    else
      socket
    end
  end

  def reset_and_start_search_task(socket, kw) do
    kw = Keyword.merge(kw, log_events: [])

    socket
    |> assign(kw)
    |> start_search_task()
  end

  def handle_info(:search, socket) do
    socket =
      socket
      |> start_search_task()

    {:noreply, socket}
  end

  def handle_info({_ref, {:search_result, %SO{} = search_op}}, socket) do
    log_events =
      search_op
      |> Map.get(:rows)
      |> Enum.map(&LogEvent.make_from_db(&1, %{source: socket.assigns.source}))

    if socket.assigns.tailing? do
      Process.send_after(self(), :search, @tailing_search_interval)
    end

    # prevents removal of log events loaded
    # during initial tailing query
    log_events =
      socket.assigns.log_events
      |> Enum.concat(log_events)
      |> Enum.reverse()
      |> Enum.take(100)
      |> Enum.reverse()

    tailing? = socket.assigns.tailing?
    querystring = socket.assigns.querystring
    log_events_empty? = log_events == []

    warning =
      cond do
        log_events_empty? and not tailing? ->
          "No logs matching your search query"

        log_events_empty? and tailing? ->
          "No logs matching your search query have been injested during last 24 hours..."

        querystring == "" and log_events_empty? and tailing? ->
          "No logs have been injested during last 24 hours..."

        true ->
          nil
      end

    {:noreply,
     assign(socket,
       log_events: log_events,
       task: nil,
       search_op: search_op,
       tailing_initial?: false,
       flash: Map.put(socket.assigns.flash, :warning, warning)
     )}
  end

  def handle_info({_ref, {:search_error, %SO{} = search_op}}, socket) do
    flash = put_in(socket.assigns.flash, [:error], format_error(search_op.error))

    {:noreply,
     assign(socket,
       log_events: [],
       flash: flash,
       task: nil,
       tailing?: false,
       tailing_initial?: false,
       search_op: search_op
     )}
  end

  def format_error(%Tesla.Env{body: body}) do
    body
    |> Poison.decode!()
    |> Map.get("error")
    |> Map.get("message")
  end

  def format_error(e), do: e

  # handles {:DOWN, ... } msgs from task
  def handle_info(_task, state) do
    {:noreply, state}
  end

  def start_search_task(%{assigns: %{querystring: nil}} = socket), do: socket

  def start_search_task(socket) do
    task =
      Task.async(fn ->
        with {:ok, search_op} <-
               SO
               |> struct(socket.assigns)
               |> Search.search() do
          {:search_result, search_op}
        else
          {:error, err} ->
            {:search_error, err}
        end
      end)

    assign(socket, task: task)
  end
end
