defmodule LogflareWeb.BillingAccountLive.ChartComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Contex.{Plot, Dataset, BarChart}
  alias Logflare.Billing.BillingCounts

  require Logger

  def mount(socket), do: {:ok, socket}

  def update(%{user: user, days: days} = _assigns, socket) do
    days = :timer.hours(24 * days)
    end_date = DateTime.utc_now()
    start_date = DateTime.add(end_date, -days, :millisecond)
    Task.async(__MODULE__, :timeseries, [user, start_date, end_date])

    socket = assign(socket, loading: true)

    socket =
      case connected?(socket) do
        true -> assign(socket, connecting: false)
        false -> assign(socket, connecting: true)
      end

    {:ok, socket}
  end

  def update(%{chart_data: data} = _assigns, socket) do
    socket =
      socket
      |> assign(chart_data: data)
      |> assign(loading: false)

    {:ok, socket}
  end

  def render(assigns) do
    ~H"""
    <div id="billing-chart" class="my-3 w-auto">
      <%= if @connecting || @loading, do: placeholder(), else: make_chart(@chart_data) %>
    </div>
    """
  end

  def make_chart(data) do
    dataset = Dataset.new(data, ["x", "y", "category"])

    content =
      BarChart.new(dataset)
      |> BarChart.data_labels(false)
      |> BarChart.colours(["5eeb8f"])

    Plot.new(400, 75, content)
    |> Plot.axis_labels("", "")
    |> Plot.titles("", "")
    |> Map.put(:margins, %{bottom: 20, left: 40, right: 40, top: 10})
    |> Plot.to_svg()
  end

  def timeseries(user, start_date, end_date) do
    data =
      user
      |> BillingCounts.timeseries(start_date, end_date)
      |> BillingCounts.timeseries_to_ext()

    {:ok, data}
  end

  defp placeholder() do
    {:safe, [~s|<svg class="loading" viewBox="0 0 400 75" role="img"></svg>|]}
  end
end
