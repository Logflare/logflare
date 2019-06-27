defmodule LogflareWeb.Source.SearchLV do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query}
  alias Logflare.{Source, Logs, LogEvent}
  alias Logflare.Logs.Search
  alias Logflare.Logs.Search.SearchOpts
  alias LogflareWeb.SourceView
  use Phoenix.LiveView

  def render(assigns) do
    Phoenix.View.render(SourceView, "search_frame.html", assigns)
  end

  def mount(%{source: source} = session, socket) do
    starts_at = Date.utc_today() |> Timex.to_datetime("Etc/UTC")
    ends_at = Timex.shift(Date.utc_today(), days: 1) |> Timex.to_datetime("Etc/UTC")

    {:ok,
     assign(socket,
       query: nil,
       loading: false,
       log_events: [],
       source: source,
       starts_at: starts_at,
       ends_at: ends_at
     )}
  end

  def handle_event("search", %{"q" => query, "partitions" => partitions} = params, socket)
      when byte_size(query) <= 100 do
    starts_at = parse_form_partition_value(partitions["starts_at"])
    ends_at = parse_form_partition_value(partitions["ends_at"])

    send(self(), :search)

    {:noreply,
     assign(socket,
       query: query,
       result: "Searching...",
       loading: true,
       partitions: {starts_at, ends_at}
     )}
  end

  def parse_form_partition_value(form_datetime) do
    fdt =
      form_datetime
      |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), v} end)
      |> Enum.into(Map.new())

    NaiveDateTime.new(fdt.year, fdt.month, fdt.day, fdt.hour, fdt.minute, fdt[:second] || 0)
  end

  def handle_info(:search, socket) do
    %{source: source, partitions: partitions, query: query} = socket.assigns

    {:ok, %{rows: log_events}} =
      Search.search(%SearchOpts{regex: query, source: source, partitions: partitions})

    log_events =
      if log_events do
        log_events
        |> Enum.map(&LogEvent.make(&1, %{source: source}))
        |> Enum.sort_by(& &1.body.timestamp, &<=/2)
      end

    {:noreply, assign(socket, loading: false, log_events: log_events)}
  end
end
