defmodule LogflareWeb.BillingAccountLive.ChartComponent do
  @moduledoc """
  Billing edit LiveView
  """

  use LogflareWeb, :live_component
  use Phoenix.HTML

  alias Contex.{Plot, Dataset, BarChart}
  alias Logflare.BillingCounts

  require Logger

  def preload(assigns) when is_list(assigns) do
    assigns
  end

  def mount(socket) do
    {:ok, socket}
  end

  def update(%{user: user, days: days} = _assigns, socket) do
    days = :timer.hours(24 * days)
    end_date = DateTime.utc_now()
    start_date = DateTime.add(end_date, -days, :millisecond)

    socket =
      socket
      |> assign(chart_data: timeseries(user, start_date, end_date))

    socket =
      case connected?(socket) do
        true ->
          assign(socket, loading: false)

        false ->
          assign(socket, loading: true)
      end

    {:ok, socket}
  end

  def render(assigns) do
    ~L"""
    <div id="billing-chart" class="my-3">
      <%= if @loading do %>
        <div class="">
        <%# live_react_component needs to be wrapped in a div or its JS code will fail!  %>
        <%= # live_react_component("Components.Loader", %{}, [id: "log-event-loader"]) %>
        </div>
      <% else %>
        <%= make_chart(@chart_data) %>
      <% end %>
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
    |> Map.put(:margins, %{bottom: 20, left: 20, right: 10, top: 10})
    |> Plot.to_svg()
  end

  defp timeseries(user, start_date, end_date) do
    BillingCounts.timeseries(user, start_date, end_date)
    |> BillingCounts.timeseries_to_ext()
  end
end
