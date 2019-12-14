defmodule LogflareWeb.Source.SearchLV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView
  alias LogflareWeb.Router.Helpers, as: Routes

  alias LogflareWeb.SourceView

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.SavedSearches
  alias __MODULE__.SearchParams
  import Logflare.Logs.Search.Utils
  require Logger
  alias Logflare.{Sources, Users}
  @tail_search_interval 5_000
  @user_idle_interval 300_000
  @default_tailing? true
  @default_chart_aggregate :count
  @default_chart_period :minute

  def render(assigns) do
    Phoenix.View.render(SourceView, "logs_search.html", assigns)
  end

  def mount(session, socket) do
    %{source_id: source_id, user_id: user_id} = session
    user = Users.Cache.get_by_and_preload(id: user_id)
    source = Sources.Cache.get_by_id_and_preload(source_id)

    Logger.info(
      "#{pid_source_to_string(self(), source)} is being mounted... Connected: #{
        connected?(socket)
      }"
    )

    socket =
      assign(
        socket,
        source: source,
        log_events: [],
        log_aggregates: [],
        loading: false,
        tailing?: true,
        tailing_paused?: nil,
        tailing_timer: nil,
        user: user,
        flash: %{},
        querystring: "",
        search_op: nil,
        search_op_error: nil,
        search_op_log_events: nil,
        search_op_log_aggregates: nil,
        search_chart_period: @default_chart_period,
        search_chart_aggregate: @default_chart_aggregate,
        tailing_timer: nil,
        user_idle_interval: @user_idle_interval,
        active_modal: nil,
        search_tip: gen_search_tip(),
        user_local_timezone: nil,
        use_local_time: true,
        search_chart_aggregate_enabled?: false,
        last_query_completed_at: nil
      )

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    log_lv_received_event("handle params", socket.assigns.source)

    querystring = params["querystring"] || params["q"] || socket.assigns.querystring

    tailing? =
      case params["tailing?"] do
        nil -> socket.assigns.tailing?
        "true" -> true
        "false" -> false
      end

    chart_period =
      case params["chart_period"] do
        nil -> socket.assigns.search_chart_period
        x -> String.to_existing_atom(x)
      end

    chart_aggregate =
      case params["chart_aggregate"] do
        nil -> socket.assigns.search_chart_aggregate
        x -> String.to_existing_atom(x)
      end

    search_chart_aggregate_enabled? = querystring =~ ~r/chart:\w+/

    socket =
      assign(
        socket,
        querystring: querystring,
        tailing?: tailing?,
        search_chart_period: chart_period,
        search_chart_aggregate: chart_aggregate,
        search_chart_aggregate_enabled?: search_chart_aggregate_enabled?
      )

    {:noreply, socket}
  end

  def handle_event("form_update" = ev, %{"search" => search}, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    params = SearchParams.new(search)

    tailing? =
      if is_nil(params[:tailing?]) do
        socket.assigns.tailing?
      else
        params[:tailing?]
      end

    querystring = params[:querystring] || ""

    chart_period =
      case search["search_chart_period"] do
        "day" -> :day
        "hour" -> :hour
        "minute" -> :minute
        "second" -> :second
        _ -> socket.assigns.search_chart_period
      end

    chart_aggregate =
      case search["search_chart_aggregate"] do
        "sum" -> :sum
        "avg" -> :avg
        "count" -> :count
        _ -> socket.assigns.search_chart_aggregate
      end

    search_chart_aggregate_enabled? = querystring =~ ~r/chart:\w+/

    warning =
      if tailing? and querystring =~ "timestamp" do
        "Timestamp field is ignored if live tail search is active"
      else
        nil
      end

    %{search_chart_aggregate: prev_chart_aggregate, search_chart_period: prev_chart_period} =
      socket.assigns

    socket =
      if {chart_aggregate, chart_period} != {prev_chart_aggregate, prev_chart_period} do
        params = %{
          chart_aggregate: "#{chart_aggregate}",
          chart_period: "#{chart_period}"
        }

        socket =
          socket
          |> assign(:querystring, querystring)
          |> assign(:search_chart_aggregate, chart_aggregate)
          |> assign(:search_chart_period, chart_period)
          |> assign(:search_chart_aggregate_enabled?, search_chart_aggregate_enabled?)
          |> assign(:log_aggregates, [])
          |> assign(:loading, true)
          |> assign(:tailing?, tailing?)

        maybe_execute_query(socket.assigns)

        live_redirect(socket,
          to: Routes.live_path(socket, __MODULE__, socket.assigns.source.id, params),
          replace: true
        )
      else
        socket
        |> assign(:querystring, querystring)
        |> assign(:search_chart_aggregate_enabled?, search_chart_aggregate_enabled?)
        |> assign(:tailing?, tailing?)
        |> assign_flash(:warning, warning)
      end

    {:noreply, socket}
  end

  def handle_event("start_search" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    params =
      socket.assigns
      |> Map.take([:search_chart_aggregate, :search_chart_period, :querystring, :tailing?])

    maybe_cancel_tailing_timer(socket)
    user_local_tz = metadata["search"]["user_local_timezone"]

    socket =
      socket
      |> assign(:log_events, [])
      |> assign(:loading, true)
      |> assign(:tailing_initial?, true)
      |> assign(:user_local_timezone, user_local_tz)
      |> assign_flash(:warning, nil)
      |> assign_flash(:error, nil)
      |> live_redirect(
        to: Routes.live_path(socket, __MODULE__, socket.assigns.source.id, params),
        replace: true
      )

    maybe_execute_query(socket.assigns)

    {:noreply, socket}
  end

  def handle_event("set_local_time" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    use_local_time =
      metadata
      |> Map.get("use_local_time")
      |> String.to_existing_atom()
      |> Kernel.not()

    socket = assign(socket, :use_local_time, use_local_time)

    socket =
      if use_local_time do
        assign(socket, :user_local_timezone, metadata["user_local_timezone"])
      else
        assign(socket, :user_local_timezone, "Etc/UTC")
      end

    {:noreply, socket}
  end

  def handle_event("activate_modal" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    modal_id = metadata["modal_id"]

    maybe_cancel_tailing_timer(socket)
    SearchQueryExecutor.cancel_query(socket.assigns)

    socket = assign(socket, :active_modal, modal_id)

    socket =
      if socket.assigns.tailing? do
        socket
        |> assign(:tailing?, false)
        |> assign(:tailing_paused?, true)
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("deactivate_modal" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    socket = assign(socket, :active_modal, nil)

    socket =
      if socket.assigns.tailing_paused? do
        socket =
          socket
          |> assign(:tailing_paused?, nil)
          |> assign(:tailing?, true)

        maybe_execute_query(socket.assigns)

        socket
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_event("remove_flash" = ev, metadata, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    key = String.to_existing_atom(metadata["flash_key"])
    socket = assign_flash(socket, key, nil)
    {:noreply, socket}
  end

  def handle_event("user_idle" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)
    socket = assign_flash(socket, :warning, "Live search paused due to user inactivity.")

    {:noreply, socket}
  end

  def handle_event("save_search" = ev, _, socket) do
    log_lv_received_event(ev, socket.assigns.source)

    case SavedSearches.insert(socket.assigns.querystring, socket.assigns.source) do
      {:ok, saved_search} ->
        socket = assign_flash(socket, :warning, "Search saved!")
        {:noreply, socket}

      {:error, changeset} ->
        {message, _} = changeset.errors[:querystring]
        socket = assign_flash(socket, :warning, "Save search error: #{message}")
        {:noreply, socket}
    end
  end

  def handle_info({:search_result, search_result}, socket) do
    log_lv_received_event("search_result", socket.assigns.source)

    tailing_timer =
      if socket.assigns.tailing? do
        log_lv(socket.assigns.source, "is scheduling tail search")
        Process.send_after(self(), :schedule_tail_search, @tail_search_interval)
      else
        nil
      end

    warning = warning_message(socket.assigns, search_result)

    log_aggregates = Enum.reverse(search_result.aggregates.rows)
    log_events = search_result.events.rows

    socket =
      socket
      |> assign(:log_events, log_events)
      |> assign(:log_aggregates, log_aggregates)
      |> assign(:search_result, search_result.events)
      |> assign(:search_op_log_events, search_result.events)
      |> assign(:search_op_log_aggregates, search_result.aggregates)
      |> assign(:tailing_timer, tailing_timer)
      |> assign(:loading, false)
      |> assign(:tailing_initial?, false)
      |> assign(:last_query_completed_at, Timex.now())
      |> assign_flash(:warning, warning)

    {:noreply, socket}
  end

  def handle_info({:search_error = msg, search_op}, socket) do
    log_lv_received_info(msg, socket.assigns.source)

    socket =
      socket
      |> assign(:search_op_error, search_op)
      |> assign_flash(:error, format_error(search_op.error))
      |> assign(:tailing?, false)
      |> assign(:loading, false)

    {:noreply, socket}
  end

  def handle_info(:schedule_tail_search = msg, socket) do
    if socket.assigns.tailing? do
      log_lv_received_info(msg, socket.assigns.source)
      maybe_execute_query(socket.assigns)
    end

    {:noreply, socket}
  end

  defp assign_flash(socket, key, message) do
    flash = socket.assigns.flash
    assign(socket, flash: put_in(flash, [key], message))
  end

  defp maybe_execute_query(assigns) do
    assigns.source.token
    |> SearchQueryExecutor.name()
    |> Process.whereis()
    |> if do
      :ok = SearchQueryExecutor.query(assigns)
    else
      Logger.error("Search Query Executor process for not alive")
    end
  end

  defp warning_message(assigns, search_op) do
    tailing? = assigns.tailing?
    querystring = assigns.querystring
    log_events_empty? = search_op.events.rows == []

    cond do
      log_events_empty? and not tailing? ->
        "No log events matching your search query."

      log_events_empty? and tailing? ->
        "No log events matching your search query ingested during last 24 hours..."

      querystring == "" and log_events_empty? and tailing? ->
        "No log events ingested during last 24 hours..."

      true ->
        nil
    end
  end

  def maybe_cancel_tailing_timer(socket) do
    if socket.assigns.tailing_timer, do: Process.cancel_timer(socket.assigns.tailing_timer)
  end
end
