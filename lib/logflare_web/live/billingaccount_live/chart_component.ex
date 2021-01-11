defmodule LogflareWeb.BillingAccountLive.ChartComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Contex.{Plot, Dataset, BarChart}
  alias Logflare.BillingCounts
  alias Logflare.User

  require Logger

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    {:ok, socket}
  end

  def update(%{counter: counter} = _assigns, socket) do
    days = :timer.hours(24 * 30)
    end_date = DateTime.utc_now()
    start_date = DateTime.add(end_date, -days, :millisecond)

    socket =
      socket
      |> assign(counter: counter)
      |> assign(chart_data: timeseries(start_date, end_date))

    socket =
      case connected?(socket) do
        true ->
          # Process.send_after(self(), {:chart_tick, counter + 1}, 100)
          assign(socket, show_chart: true)

        false ->
          assign(socket, show_chart: false)
      end

    {:ok, socket}
  end

  def render(assigns) do
    ~L"""
    <div id="billing-chart">
      <%= make_chart(@chart_data) %>
    </div>
    """
  end

  def make_chart(data) do
    plot_options = plot_options()

    content =
      Dataset.new(data)
      |> BarChart.new()
      |> BarChart.data_labels(false)
      |> IO.inspect()

    Plot.new(400, 75, content)
    |> Plot.axis_labels("", "")
    |> Plot.titles("", "")
    |> IO.inspect()
    |> Map.put(:margins, %{bottom: 20, left: 20, right: 10, top: 10})
    |> Plot.to_svg()
  end

  defp plot_options() do
    %{top_margin: 10, right_margin: 10, bottom_margin: 20, left_margin: 20}
  end

  defp timeseries(start_date, end_date) do
    BillingCounts.timeseries(%User{id: 36}, start_date, end_date)
    |> BillingCounts.timeseries_to_ext()
    |> Enum.reverse()
  end
end
